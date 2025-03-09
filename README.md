# 🏭 Projet Data Warehouse ETL

## 📋 Vue d'ensemble

Ce projet implémente un entrepôt de données complet avec un pipeline ETL (Extract, Transform, Load) pour analyser des données de ventes. Le système extrait les données depuis des fichiers sources, les transforme selon un modèle en étoile, et les charge dans une base de données PostgreSQL pour permettre des analyses avancées.

## ✨ Fonctionnalités principales

- 📥 **Pipeline ETL robuste** : Extraction, transformation et chargement des données
- 📊 **Analyses automatisées** : Génération de graphiques et rapports sur les ventes
- 📈 **Tableaux de bord** : Visualisation interactive des indicateurs clés
- 🔄 **Mise à jour planifiée** : Actualisation automatique des données
- 📝 **Journalisation détaillée** : Suivi complet des opérations

## 🏗️ Structure du projet

```
myproject_data_warehouse/
├── config/              # Fichiers de configuration
├── data/                # Données sources et générées
├── dashboard/           # Interface de visualisation 
├── db/                  # Scripts de base de données
├── docs/                # Documentation détaillée
├── logs/                # Journaux d'exécution
├── results/             # Résultats d'analyses
│   └── visualisations/  # Graphiques et rapports générés
├── scripts/             # Scripts d'analyse et utilitaires
├── src/                 # Code source principal
│   ├── etl/             # Composants du pipeline ETL
│   ├── utils/           # Fonctions utilitaires
│   └── visualization/   # Générateurs de visualisations
└── run.py               # Point d'entrée principal
```

## 🚀 Installation et démarrage

### Prérequis

- 🐍 Python 3.8+
- 🐘 PostgreSQL 12+
- 📦 Packages Python (voir `requirements.txt`)

### Installation

1. **Cloner le dépôt**

```bash
git clone [url-du-dépôt]
cd myproject_data_warehouse
```

2. **Installer les dépendances**

```bash
pip install -r requirements.txt
```

3. **Configurer la base de données**

Assurez-vous que PostgreSQL est en cours d'exécution et créez une base de données nommée `data_warehouse`.

4. **Configuration**

Modifiez les paramètres de connexion à PostgreSQL dans `src/utils/config.yaml` si nécessaire:

```yaml
database:
  user: 'postgres'
  password: 'admin'  # À modifier selon votre configuration
  host: 'localhost'
  port: '5432'
  name: 'data_warehouse'
```

## 📌 Architecture du pipeline ETL

### 🧩 Vue d'ensemble de l'architecture

Notre architecture ETL suit un modèle minimaliste et se concentre sur les fonctionnalités essentielles :

1. **📥 Extraction** : Lecture des fichiers CSV sources de façon robuste
2. **⚙️ Transformation** : Conversions et nettoyage des données
3. **📤 Chargement** : Insertion dans PostgreSQL avec gestion d'erreurs

### 🔄 Flux de données

Le processus ETL suit ce flux de données:

1. 📥 **Extraction** : Lecture des fichiers CSV depuis le dossier `data/`
   - 👥 `customers_dim.csv`: Données des clients
   - 📦 `products_dim.csv`: Données des produits
   - 🏬 `stores_dim.csv`: Données des magasins
   - 📅 `time_dim.csv`: Dimension temporelle
   - 💰 `sales_fact.csv`: Table de faits des ventes

2. ⚙️ **Transformation**
   - 🧹 **Nettoyage** : Suppression des espaces, conversion des types, gestion des valeurs manquantes
   - ✅ **Validation** : Détection des doublons, correction des valeurs négatives, vérification de cohérence
   - 🔐 **Intégrité** : Validation des clés étrangères, suppression des enregistrements invalides

3. 📤 **Chargement**
   - 🗄️ Création du schéma en étoile dans PostgreSQL
   - 📊 Insertion dans les tables dimensionnelles (Customers, Products, Stores, Time)
   - 📈 Insertion dans la table de faits (Sales)
   - 🔍 Création d'index pour optimiser les performances

### 🧩 Composants Principaux

- 🔄 **Pipeline ETL** (`src/etl/pipeline.py`) : Classe `SimpleETL` qui encapsule la logique du pipeline
- 💾 **Chargeur SQL** (`src/etl/loaders.py`) : Interface pour charger les données dans PostgreSQL
- ⚙️ **Configuration** (`src/utils/config.yaml`) : Paramètres essentiels du système

## 📊 Utilisation

### ▶️ Exécution complète

Pour lancer le projet complet (ETL + analyse) :

```bash
python run.py
```

### 🔄 Pipeline ETL uniquement

Pour exécuter uniquement le pipeline ETL :

```bash
python src/etl/pipeline.py
```

### 📊 Analyses uniquement

Pour générer seulement les analyses et visualisations :

```bash
python scripts/analyze_data.py
```

### ⏱️ Exécution planifiée

Pour démarrer le planificateur ETL qui exécutera le pipeline automatiquement :

```bash
python scripts/schedule_etl.py
```

Le pipeline s'exécutera:
- ⚡ Une fois immédiatement au lancement du script (pour tester)
- 🌙 Tous les jours à 2h du matin

Vous pouvez modifier la fréquence dans le fichier `scripts/schedule_etl.py`.

## 📈 Monitoring et Performances

### 📊 Logs et suivi

Le pipeline génère des logs détaillés:
- 📝 `logs/etl_pipeline.log`: Logs du processus ETL
- 📝 `logs/scheduler.log`: Logs du planificateur

Chaque exécution est également enregistrée dans la table `ETL_Log` de la base de données.

### ⚡ Optimisations de performance

Le pipeline est optimisé pour un traitement efficace:
- 🔄 Chargement par lots pour réduire les transactions
- 🗜️ Compression des données pour améliorer la vitesse de transfert
- 🔍 Utilisation d'index pour accélérer les requêtes
- 🧵 Parallélisation de certaines opérations (lorsque possible)

## 🔧 Dépannage

Si vous rencontrez des problèmes :

1. **🔴 Erreur de connexion à PostgreSQL**
   - Vérifiez que le service PostgreSQL est actif
   - Confirmez les paramètres de connexion dans `src/utils/config.yaml`

2. **⚠️ Données manquantes ou incomplètes**
   - Vérifiez l'intégrité des fichiers CSV sources
   - Consultez les logs pour identifier les enregistrements rejetés

3. **🐌 Performances lentes**
   - Vérifiez les index de la base de données
   - Réduisez la taille des lots si nécessaire
   - Augmentez les ressources allouées à PostgreSQL

## 🔮 Évolutions futures

Ce projet peut être enrichi progressivement avec:

1. 🔄 **Mode incrémental** : Traitement uniquement des nouvelles données
2. ✅ **Validations avancées** : Contrôles de qualité plus poussés
3. 📝 **Journalisation enrichie** : Suivi plus détaillé des opérations
4. 📊 **Tableau de bord amélioré** : Visualisations interactives plus complètes
5. 🔄 **Flux de travail complexes** : Utilisation d'Apache Airflow pour l'orchestration
6. 📚 **Historisation des données** : Mise en place d'une stratégie SCD (Slowly Changing Dimensions)

## ✅ Avantages de l'approche

- **🧩 Simplicité** : Code facile à comprendre et à maintenir
- **🛡️ Robustesse** : Gestion d'erreurs améliorée pour éviter les interruptions
- **🧱 Modularité** : Structure permettant d'ajouter facilement de nouvelles fonctionnalités
- **⚡ Performance** : Traitement des données optimisé pour les grandes quantités

## 📜 Licence

Ce projet est sous licence MIT.