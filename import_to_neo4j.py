from pymongo import MongoClient
from neo4j import GraphDatabase

# CONFIG - à adapter selon ton environnement
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "entertainment"
COLLECTION_NAME = "films"

NEO4J_URI = "bolt://52.205.250.203:7687"  # ou "neo4j+s://xxx.bolt.neo4jsandbox.com:443"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "sign-talks-recombinations"

# Membres du projet à rattacher à un film
PROJECT_MEMBERS = ["Fenosoa", "Coéquipier1", "Coéquipier2", "Mariam"]
MOVIE_FOR_MEMBERS = "Inception"  # Nom du film auquel on les rattache

def import_data():
    # --- Connexion MongoDB ---
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    films_collection = db[COLLECTION_NAME]

    # --- Connexion Neo4j ---
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        # 1) (Optionnel) Vider la base Neo4j pour repartir de zéro
        session.run("MATCH (n) DETACH DELETE n")
        print("Base Neo4j réinitialisée.")

        # 2) Parcourir les films dans MongoDB et créer les nœuds
        for film in films_collection.find():
            # Récupérer les champs
            film_id = str(film.get("_id", ""))       # _id du film
            title = film.get("title", "Unknown")    
            year = film.get("year", None)
            votes = film.get("Votes", 0)
            revenue = film.get("Revenue (Millions)", 0)
            rating = film.get("rating", "Unknown")
            director = film.get("Director", "Unknown")
            
            # Certains champs comme Actors sont une liste ou une string "A, B, C"
            actors_str = film.get("Actors", "")
            actors_list = actors_str.split(", ") if actors_str else []

            # --- Créer le nœud Film ---
            session.run("""
                MERGE (f:Film {id: $film_id})
                SET f.title = $title,
                    f.year = $year,
                    f.votes = $votes,
                    f.revenue = $revenue,
                    f.rating = $rating,
                    f.director = $director
            """, film_id=film_id, title=title, year=year, votes=votes, 
                 revenue=revenue, rating=rating, director=director)

            # --- Créer le nœud Realisateur et la relation :REALISE ---
            session.run("""
                MERGE (r:Realisateur {name: $director})
                MERGE (f:Film {id: $film_id})
                MERGE (r)-[:REALISE]->(f)
            """, director=director, film_id=film_id)

            # --- Créer les nœuds Actor + relation :A_JOUE ---
            for actor_name in actors_list:
                session.run("""
                    MERGE (a:Actor {name: $actor_name})
                    MERGE (f:Film {id: $film_id})
                    MERGE (a)-[:A_JOUE]->(f)
                """, actor_name=actor_name, film_id=film_id)

        # 3) Ajouter les membres du projet comme acteurs d'un film précis
        for member in PROJECT_MEMBERS:
            session.run("""
                MERGE (a:Actor {name: $member})
                MERGE (f:Film {title: $movie_title})
                MERGE (a)-[:A_JOUE]->(f)
            """, member=member, movie_title=MOVIE_FOR_MEMBERS)

    driver.close()
    mongo_client.close()
    print("✅ Importation terminée !")

if __name__ == "__main__":
    import_data()
