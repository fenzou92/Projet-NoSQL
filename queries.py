from database import get_mongo_collection
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import scipy.stats as stats


def q1_most_movies_year():
    """
    1. Afficher l’année où le plus grand nombre de films ont été sortis.
    Cette fonction utilise une agrégation MongoDB pour regrouper les films par année,
    compter le nombre de films par année, trier par ordre décroissant, puis retourner l'année
    avec le plus grand nombre de films.
    """
    # Récupération de la collection MongoDB
    collection = get_mongo_collection()
    # Définition du pipeline d'agrégation :
    # - Groupement par année et comptage des films
    # - Tri par nombre décroissant
    # - Limitation au premier résultat
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
    Cette fonction compte simplement le nombre de documents dont l'année est supérieure à 1999.
    """
    collection = get_mongo_collection()
    count = collection.count_documents({"year": {"$gt": 1999}})
    return count

def q3_avg_votes_2007():
    """
    3. Quelle est la moyenne des votes des films sortis en 2007 ?
    Ici, on filtre les films de l'année 2007 puis on calcule la moyenne des votes.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$match": {"year": 2007}},  # Filtre sur l'année 2007
        {"$group": {"_id": None, "avg_votes": {"$avg": "$Votes"}}}  # Calcule la moyenne des votes
    ]
    result = list(collection.aggregate(pipeline))
    if result:
        return result[0]["avg_votes"]
    return 0

def q4_histogram_films_by_year():
    """
    4. Afficher un histogramme du nombre de films par année.
       La fonction retourne une figure générée avec Matplotlib/Seaborn.
    """
    collection = get_mongo_collection()
    # Pipeline pour regrouper les films par année et compter le nombre de films par année
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}  # Tri croissant par année
    ]
    data = list(collection.aggregate(pipeline))
    # Extraction des années et des comptes correspondants, en excluant les années manquantes
    years = [d["_id"] for d in data if d["_id"] is not None]
    counts = [d["count"] for d in data if d["_id"] is not None]
    
    # Création du graphique
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(x=years, y=counts, ax=ax)
    ax.set_xlabel("Année")
    ax.set_ylabel("Nombre de films")
    ax.set_title("Nombre de films par année")
    plt.xticks(rotation=45)
    plt.tight_layout()  # Ajuste automatiquement les marges
    return fig


def q5_available_genres():
    """
    5. Quelles sont les genres de films disponibles dans la base ?
       Retourne la liste distincte des genres.
       Attention : si 'genre' est une chaîne comme "Comedy, Drama", la fonction distinct ne les séparera pas.
    """
    collection = get_mongo_collection()
    # Récupère les valeurs distinctes pour le champ "genre"
    distinct_genres = collection.distinct("genre")
    return distinct_genres

def q6_max_revenue_film():
    """
    6. Quel est le film qui a généré le plus de revenu ?
       On suppose que le champ 'Revenue (Millions)' existe et est numérique.
    """
    collection = get_mongo_collection()
    # Tri des films par revenu décroissant et limitation au premier résultat
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
       On regroupe les films par réalisateur et on filtre ceux qui ont un compte supérieur à 5.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},  # Sélectionne les réalisateurs avec plus de 5 films
        {"$sort": {"count": -1}}
    ]
    result = list(collection.aggregate(pipeline))
    return result

def q8_genre_highest_avg_revenue():
    """
    8. Quel est le genre de film qui rapporte en moyenne le plus de revenus ?
       On agrège directement sur le champ 'genre' (ou après un unwind si c'est un tableau).
    """
    collection = get_mongo_collection()
    pipeline = [
        # Si 'genre' est un tableau, décommente la ligne suivante :
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
    9. Quels sont les 3 films les mieux notés par décennie, basés sur le Metascore ?
    La fonction regroupe les films par décennie, trie par Metascore décroissant,
    et extrait les trois premiers films pour chaque décennie.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$match": {"Metascore": {"$ne": None}}},  # Exclut les films sans Metascore
        {"$project": {
            "title": 1,
            "Metascore": 1,
            "decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}
        }},
        {"$sort": {"decade": 1, "Metascore": -1}},
        {"$group": {
            "_id": "$decade",
            "top3": {"$push": "$$ROOT"}  # Conserve tous les films, on extraira les 3 premiers ensuite
        }},
        {"$project": {
            "top3": {"$slice": ["$top3", 3]}  # Ne garde que les 3 films les mieux notés par décennie
        }}
    ]
    data = list(collection.aggregate(pipeline))
    return data

def q10_longest_film_by_genre():
    """
    10. Quel est le film le plus long (Runtime) par genre ?
       On suppose que 'Runtime' est numérique et 'genre' est un champ unique ou un tableau.
    """
    collection = get_mongo_collection()
    pipeline = [
        # Si 'genre' est un tableau, décommente la ligne suivante :
        # {"$unwind": "$genre"},
        {"$sort": {"Runtime (Minutes)": -1}},  # Trie les films par durée décroissante
        {"$group": {
            "_id": "$genre",
            "film": {"$first": "$$ROOT"}  # Prend le premier film de chaque groupe (le plus long)
        }}
    ]
    data = list(collection.aggregate(pipeline))
    return data

def q11_create_view_high_quality():
    """
    11. Créer une vue MongoDB affichant uniquement les films ayant une note supérieure à 80
        (Metascore) et généré plus de 50 millions de dollars.
        La vue est créée via la commande 'database.command()'.
    """
    coll = get_mongo_collection()
    db = coll.database  # Récupère l'objet database
    view_name = "high_quality_films"  # Nom de la vue à créer
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

def q12_runtime_revenue_data():
    """
    Retourne un DataFrame contenant les colonnes 'Runtime (Minutes)' et 'Revenue (Millions)'
    pour les films, en excluant les documents qui n'ont pas de valeurs valides.
    """
    collection = get_mongo_collection()
    # On récupère les documents en ne sélectionnant que les champs d'intérêt (excluant _id)
    data = list(collection.find({}, {"Runtime (Minutes)": 1, "Revenue (Millions)": 1, "_id": 0}))
    df = pd.DataFrame(data).dropna()  # Supprime les lignes avec des valeurs manquantes
    
    # Conversion des colonnes en types numériques pour assurer la cohérence des données
    df["Runtime (Minutes)"] = pd.to_numeric(df["Runtime (Minutes)"], errors="coerce")
    df["Revenue (Millions)"] = pd.to_numeric(df["Revenue (Millions)"], errors="coerce")
    df = df.dropna()  # Retire de nouveau les lignes non convertibles
    
    return df

def q12_runtime_revenue_correlation():
    """
    Calcule et renvoie la corrélation de Pearson et la p-value
    pour la relation entre la durée (Runtime) et le revenu (Revenue) des films,
    en se basant sur le DataFrame obtenu par q12_runtime_revenue_data().
    """
    df = q12_runtime_revenue_data()
    if len(df) < 2:
        return 0, 1  # Retourne des valeurs par défaut si les données sont insuffisantes
    corr, p_value = stats.pearsonr(df["Runtime (Minutes)"], df["Revenue (Millions)"])
    return corr, p_value

def q13_evolution_avg_runtime_per_decade():
    """
    13. Y a-t-il une évolution de la durée moyenne des films par décennie ?
       La fonction regroupe les films par décennie et calcule la durée moyenne (Runtime)
       pour chaque décennie.
    """
    collection = get_mongo_collection()
    pipeline = [
        {"$project": {
            "Runtime (Minutes)": 1,
            # Calcul de la décennie en soustrayant le reste de la division par 10
            "decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}
        }},
        {"$group": {
            "_id": "$decade",
            "avg_runtime": {"$avg": "$Runtime (Minutes)"}
        }},
        {"$sort": {"_id": 1}}  # Tri par décennie croissante
    ]
    data = list(collection.aggregate(pipeline))
    return data

def q18_genre_most_represented_mongo():
    """
    18. Genre le plus représenté dans la base (version corrigée)
       La fonction regroupe les films par genre et renvoie le genre ayant le plus grand nombre d'occurrences.
    """
    try:
        collection = get_mongo_collection()
        pipeline = [
            {"$match": {"genre": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        result = list(collection.aggregate(pipeline))
        return (result[0]["_id"], result[0]["count"]) if result else (None, 0)
    except Exception as e:
        print(f"Erreur MongoDB : {str(e)}")
        return None, 0
