import json

# Ouvrir le fichier JSON ligne par ligne et transformer en liste
with open("movies.json", "r", encoding="utf-8") as f:
    movies = [json.loads(line) for line in f]

# Écrire le fichier en format tableau JSON correct
with open("movies_fixed.json", "w", encoding="utf-8") as f:
    json.dump(movies, f, indent=4)

print("✅ Fichier corrigé : movies_fixed.json")
