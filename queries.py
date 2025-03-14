from database import get_mongo_collection
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import scipy.stats as stats

# =========== Existant (questions 1 à 4) ===========

def q1_most_movies_year():
    """
    1. Afficher l’année où le plus grand nombre de films ont été sortis.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(collection.aggregate(pipeline))
    if result:
        return result[0]["_id"], result[0]["count"]
    return None, 0

def q2_films_after_1999():
    """
    2. Quel est le nombre de films sortis après l’année 1999 ?
    """
    collection = get_mongo_collection()
    count = collection.count_documents({"year": {"$gt": 1999}})
    return count

def q3_avg_votes_2007():
    """
    3. Quelle est la moyenne des votes des films sortis en 2007 ?
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$match": {"year": 2007}},
        {"$group": {"_id": None, "avg_votes": {"$avg": "$Votes"}}}
    ]
    result = list(collection.aggregate(pipeline))
    if result:
        return result[0]["avg_votes"]
    return 0

def q4_histogram_films_by_year():
    """
    4. Afficher un histogramme du nombre de films par année.
       On retourne la figure Matplotlib/Seaborn.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    data = list(collection.aggregate(pipeline))
    years = [d["_id"] for d in data if d["_id"] is not None]
    counts = [d["count"] for d in data if d["_id"] is not None]
    
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(x=years, y=counts, ax=ax)
    ax.set_xlabel("Année")
    ax.set_ylabel("Nombre de films")
    ax.set_title("Nombre de films par année")
    plt.xticks(rotation=45)
    plt.tight_layout()
    return fig

# =========== NOUVEAU (questions 5 à 13) ===========

def q5_available_genres():
    """
    5. Quelles sont les genres de films disponibles dans la base ?
       Retourne la liste distincte des genres.
       Attention : si 'genre' est une string "Comedy, Drama", distinct ne les séparera pas.
    """
    collection = get_mongo_collection()
    distinct_genres = collection.distinct("genre")  # s'il s'agit d'un champ simple
    return distinct_genres

def q6_max_revenue_film():
    """
    6. Quel est le film qui a généré le plus de revenu ?
       Suppose que le champ 'Revenue (Millions)' existe et est numérique.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$sort": {"Revenue (Millions)": -1}},
        {"$limit": 1}
    ]
    data = list(collection.aggregate(pipeline))
    if data:
        film = data[0]
        return film["title"], film.get("Revenue (Millions)", 0)
    return None, 0

def q7_directors_more_than_5():
    """
    7. Quels sont les réalisateurs ayant réalisé plus de 5 films dans la base de données ?
       Regroupe par 'Director' et filtre ceux qui ont count > 5.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},
        {"$sort": {"count": -1}}
    ]
    result = list(collection.aggregate(pipeline))
    # result : [{"_id":"Christopher Nolan","count":7}, ...]
    return result

def q8_genre_highest_avg_revenue():
    """
    8. Quel est le genre de film qui rapporte en moyenne le plus de revenus ?
       Suppose que 'genre' est un champ unique ou un tableau. 
       Si c'est un champ unique, on agrège directement, sinon on fait un $unwind.
    """
    collection = get_mongo_collection()
    pipeline = [
        # Si 'genre' est un tableau, active cette ligne :
        # {"$unwind": "$genre"},
        
        {"$group": {
            "_id": "$genre",
            "avg_revenue": {"$avg": "$Revenue (Millions)"}
        }},
        {"$sort": {"avg_revenue": -1}},
        {"$limit": 1}
    ]
    data = list(collection.aggregate(pipeline))
    if data:
        return data[0]["_id"], data[0]["avg_revenue"]
    return None, 0

def q9_top_3_rated_per_decade():
    """
    9. Quels sont les 3 films les mieux notés (rating) pour chaque décennie ?
       Suppose que 'rating' est numérique. 
       Sinon, adapter pour le champ de notation (metascore ?).
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$project": {
            "title": 1,
            "rating": 1,
            "decade": {
                "$subtract": ["$year", {"$mod": ["$year", 10]}]
            }
        }},
        {"$sort": {"decade": 1, "rating": -1}},
        {"$group": {
            "_id": "$decade",
            "films": {"$push": "$$ROOT"}
        }},
        {"$project": {
            "top3": {"$slice": ["$films", 3]}
        }}
    ]
    data = list(collection.aggregate(pipeline))
    # data : [{"_id":1990,"top3":[{title:..., rating:..., decade:1990},...]}]
    return data

def q10_longest_film_by_genre():
    """
    10. Quel est le film le plus long (Runtime) par genre ?
       Suppose que 'Runtime' est numérique et 'genre' est un champ unique ou tableau.
    """
    collection = get_mongo_collection()
    pipeline = [
        # si 'genre' est un tableau, décommente cette ligne :
        # {"$unwind":"$genre"},
        
        {"$sort": {"Runtime (Minutes)": -1}},
        {"$group": {
            "_id": "$genre",
            "film": {"$first": "$$ROOT"}
        }}
    ]
    data = list(collection.aggregate(pipeline))
    # each doc => _id: "Action", film: {title:..., Runtime (Minutes):...}
    return data

def q11_create_view_high_quality():
    """
    11. Créer une vue MongoDB affichant uniquement les films ayant une note supérieure à 80
        (Metascore) et généré plus de 50 millions de dollars.
        On peut créer la vue via 'database.command()'.
    """
    coll = get_mongo_collection()
    db = coll.database  # récupère la database
    # Nom de la vue : "high_quality_films"
    view_name = "high_quality_films"
    try:
        db.command({
            "create": view_name,
            "viewOn": coll.name,
            "pipeline": [
                {"$match": {
                    "Metascore": {"$gt": 80},
                    "Revenue (Millions)": {"$gt": 50}
                }}
            ]
        })
        return f"Vue '{view_name}' créée avec succès."
    except Exception as e:
        return f"Erreur lors de la création de la vue: {e}"

def q12_runtime_revenue_correlation():
    """
    12. Calculer la corrélation entre la durée des films (Runtime) et leur revenu (Revenue).
        (Réaliser une analyse statistique).
    """
    collection = get_mongo_collection()
    data = list(collection.find({}, {"Runtime (Minutes)": 1, "Revenue (Millions)": 1, "_id": 0}))
    df = pd.DataFrame(data)
    # Retirer les lignes sans valeurs
    df = df.dropna()
    # Vérifier que les champs sont bien numériques
    df["Runtime (Minutes)"] = pd.to_numeric(df["Runtime (Minutes)"], errors="coerce")
    df["Revenue (Millions)"] = pd.to_numeric(df["Revenue (Millions)"], errors="coerce")
    df = df.dropna()
    
    if len(df) < 2:
        return 0, 1  # pas assez de données
    
    corr, p_value = stats.pearsonr(df["Runtime (Minutes)"], df["Revenue (Millions)"])
    return corr, p_value

def q13_evolution_avg_runtime_per_decade():
    """
    13. Y a-t-il une évolution de la durée moyenne des films par décennie ?
       On regroupe par décennie et on calcule la durée moyenne (Runtime).
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$project": {
            "Runtime (Minutes)": 1,
            "decade": {
                "$subtract": ["$year", {"$mod": ["$year", 10]}]
            }
        }},
        {"$group": {
            "_id": "$decade",
            "avg_runtime": {"$avg": "$Runtime (Minutes)"}
        }},
        {"$sort": {"_id": 1}}
    ]
    data = list(collection.aggregate(pipeline))
    # Ex. [{"_id":1990,"avg_runtime":120.5}, ...]
    return data
