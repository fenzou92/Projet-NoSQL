from config import get_database

# Connexion à la base MongoDB
db = get_database()

# Vérification de la connexion
print(f"Connexion réussie à la base : {db.name}")

# Afficher le nombre de documents dans la collection
collection = db["films"]
print(f"Nombre de films : {collection.count_documents({})}")
