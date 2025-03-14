from pymongo import MongoClient
import json

# Connexion à MongoDB
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client["entertainment"]
films_collection = db["films"]

# Extraction des films
films = list(films_collection.find({}, {"_id": 1, "title": 1, "year": 1, "Votes": 1, "Revenue (Millions)": 1, "rating": 1, "Director": 1, "Actors": 1}))

# Extraction des réalisateurs distincts
realisateurs = list(films_collection.distinct("Director"))

# Extraction des acteurs distincts
actors_set = set()
for film in films:
    actors = film.get("Actors", "").split(", ")
    actors_set.update(actors)
actors_list = list(actors_set)

# Sauvegarde des fichiers JSON pour import dans Neo4j
with open("films.json", "w", encoding="utf-8") as f:
    json.dump(films, f, indent=4)

with open("realisateurs.json", "w", encoding="utf-8") as f:
    json.dump(realisateurs, f, indent=4)

with open("actors.json", "w", encoding="utf-8") as f:
    json.dump(actors_list, f, indent=4)

print("Exportation terminée avec succès !")
