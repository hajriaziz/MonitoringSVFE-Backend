# Utiliser une image de base officielle Python
FROM python:3.9-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier des dépendances dans le conteneur
COPY requirements.txt .

# Installer les dépendances du projet
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le contenu de l'application dans le conteneur
COPY . .

# Exposer le port sur lequel l'application s'exécute
EXPOSE 8000

# Commande pour lancer l'application
CMD ["python", "main.py"]
