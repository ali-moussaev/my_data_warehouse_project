import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from sqlalchemy import create_engine, text

# Configuration de l'encodage pour les caractères accentués
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Création du dossier pour les visualisations
if not os.path.exists('visualisations'):
    os.makedirs('visualisations')

# Reste du code... mport pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from sqlalchemy import create_engine, text

# Configuration de l'encodage pour les caractères accentués
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Création du dossier pour les visualisations
if not os.path.exists('visualisations'):
    os.makedirs('visualisations')

# Reste du code... 