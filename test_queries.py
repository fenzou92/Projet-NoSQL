from queries import get_all_movies, get_movie_by_title, add_movie, update_movie, delete_movie

# Tester la récupération des films
print("🔹 Liste des films :")
print(get_all_movies(3))

# Tester la recherche d'un film
print("\n🔹 Recherche du film 'Inception' :")
print(get_movie_by_title("Inception"))

# Ajouter un film
new_movie = {
    "title": "Test Movie",
    "genre": "Sci-Fi",
    "year": 2023
}
print("\n🔹 Ajout d'un film :")
print(add_movie(new_movie))

# Mettre à jour un film
print("\n🔹 Mise à jour du film 'Test Movie' :")
print(update_movie("Test Movie", {"year": 2024}))

# Supprimer un film
print("\n🔹 Suppression du film 'Test Movie' :")
print(delete_movie("Test Movie"))
