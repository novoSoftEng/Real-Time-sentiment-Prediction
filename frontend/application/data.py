import pandas as pd
from pymongo import MongoClient

# Établir une connexion à MongoDB
client = MongoClient('localhost', 27017)
db = client['twitter_database']
collection = db['twitter_collection']

# Lire le fichier CSV en spécifiant les noms de colonnes
column_names = ['Tweet ID', 'Entity', 'Sentiment', 'Tweet content']
df = pd.read_csv('C:\\Users\\IMANE\\Desktop\\twitter_training.csv', names=column_names)

# Convertir le DataFrame en dictionnaire pour l'insertion dans MongoDB
data = df.to_dict(orient='records')

# Insérer les données dans la collection MongoDB
collection.insert_many(data)

print("Données insérées avec succès dans MongoDB.")
