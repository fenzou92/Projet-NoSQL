from pymongo import MongoClient
from neo4j import GraphDatabase
import re

# === Configuration MongoDB ===
MONGO_URI = "mongodb+srv://feno:u1hcslgM3AkFGOru@cluster0.l1dryix.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "entertainment"
COLLECTION_NAME = "films"

# === Configuration Neo4j ===
NEO4J_URI = "bolt://54.210.151.222:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "notice-rates-definitions"

PROJECT_MEMBERS = ["Fenosoa", "Coéquipier1", "Coéquipier2", "Mariam"]
MOVIE_FOR_MEMBERS = "Inception"

def normalize_genres(raw_genres):
    """Gestion spécifique pour les chaînes séparées par des virgules"""
    if isinstance(raw_genres, str):
        genres = [g.strip().title() for g in re.split(r',\s*', raw_genres)]
    elif isinstance(raw_genres, list):
        genres = [str(g).strip().title() for g in raw_genres]
    else:
        genres = ["Inconnu"]
    return list({g for g in genres if g} or {"Inconnu"})

def import_data():
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    films_collection = db[COLLECTION_NAME]

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        print("Base Neo4j réinitialisée.")

        batch = []
        for film in films_collection.find():
            # Gestion de la conversion du revenue
            rev_raw = film.get("Revenue (Millions)", 0)
            if isinstance(rev_raw, str):
                if rev_raw.strip() == "":
                    rev = 0.0
                else:
                    rev = float(rev_raw.replace("$", ""))
            else:
                rev = float(rev_raw)
                
            film_data = {
                "film_id": str(film["_id"]),
                "title": film.get("title", "Sans titre").strip(),
                "director": film.get("Director", "Inconnu").strip().title(),
                "actors": [a.strip().title() for a in film.get("Actors", "").split(", ") if a],
                "genres": normalize_genres(film.get("genre", "")),
                "year": film.get("year", 0),
                "rating": film.get("rating", "NC").upper(),
                "votes": int(film.get("Votes", 0)) if str(film.get("Votes", 0)).isdigit() else 0,
                "revenue": rev
            }
            batch.append(film_data)

            if len(batch) >= 50:
                process_batch(session, batch)
                batch = []

        if batch:
            process_batch(session, batch)

        # Ajout des membres du projet
        session.run("""
            UNWIND $members AS member
            MERGE (a:Actor {name: member})
            MERGE (f:Film {title: $title})
            MERGE (a)-[:A_JOUE]->(f)
        """, members=PROJECT_MEMBERS, title=MOVIE_FOR_MEMBERS)

    driver.close()
    mongo_client.close()
    print("✅ Importation réussie avec données financières !")

def process_batch(session, batch):
    """Requête Cypher avec propriétés financières"""
    query = """
    UNWIND $batch AS movie
    MERGE (f:Film {id: movie.film_id})
    SET f.title = movie.title,
        f.year = movie.year,
        f.rating = movie.rating,
        f.votes = movie.votes,
        f.revenue = movie.revenue
        
    WITH movie, f
    MERGE (r:Realisateur {name: movie.director})
    MERGE (r)-[:REALISE]->(f)
    
    WITH movie, f
    UNWIND movie.actors AS actor
    MERGE (a:Actor {name: actor})
    MERGE (a)-[:A_JOUE]->(f)
    
    WITH movie, f
    UNWIND movie.genres AS genre
    MERGE (g:Genre {name: genre})
    MERGE (f)-[:A_POUR_GENRE]->(g)
    """
    session.run(query, batch=batch)

if __name__ == "__main__":
    import_data()
