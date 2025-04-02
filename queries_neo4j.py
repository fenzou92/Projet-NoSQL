"""
Ce fichier contient des fonctions Python pour répondre aux questions 14 à 30
en exécutant des requêtes Cypher sur un graphe Neo4j.

Labels utilisés : (Actor), (Film), (Realisateur)
Relations utilisées :
  - (Actor)-[:A_JOUE]->(Film)
  - (Realisateur)-[:REALISE]->(Film)
Propriétés exploitées :
  - f.year
  - f.genre
  - f.Votes
  - f.`Revenue (Millions)` ou f.revenue
  - f.Metascore

L'approche consiste à ouvrir une session avec le driver Neo4j, exécuter une requête Cypher
pour répondre à une question, puis retourner les résultats formatés.
"""

from neo4j import Driver

########################
# Q14
########################
def q14_actor_most_films(driver: Driver):
    """
    14. Quel est l'acteur ayant joué dans le plus grand nombre de films ?
    
    - La requête recherche tous les nœuds Actor liés aux nœuds Film par la relation A_JOUE.
    - Elle compte le nombre de films associés à chaque acteur.
    - Le résultat est trié par ordre décroissant afin de retourner l'acteur avec le plus de films.
    """
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    RETURN a.name AS actor, COUNT(f) AS nbFilms
    ORDER BY nbFilms DESC
    LIMIT 1
    """
    with driver.session() as session:
        # Exécution de la requête et récupération du premier résultat
        record = session.run(query).single()
        if record:
            return record["actor"], record["nbFilms"]
        return None, 0

########################
# Q15
########################
def q15_actor_most_films_with_anne_hathaway(driver: Driver):
    """
    15. Quels sont les acteurs ayant joué le plus de films en commun avec l'actrice Anne Hathaway ?
       On suppose que le nœud Actor avec le nom "Anne Hathaway" existe.
       
    - La requête identifie d'abord Anne Hathaway et les films dans lesquels elle a joué.
    - Ensuite, elle trouve les autres acteurs ayant joué dans ces mêmes films.
    - Enfin, elle compte le nombre de films en commun pour chaque acteur (hors Anne Hathaway),
      trie par ce nombre, et retourne l'acteur avec le maximum.
    """
    query = """
    MATCH (anne:Actor {name:"Anne Hathaway"})-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    WHERE a <> anne
    RETURN a.name AS actor, COUNT(f) AS nbCommonFilms
    ORDER BY nbCommonFilms DESC
    LIMIT 1
    """
    with driver.session() as session:
        record = session.run(query).single()
        if record:
            return record["actor"], record["nbCommonFilms"]
        return None, 0

########################
# Q16
########################
def q16_actor_most_revenue(driver):
    """
    16. Acteur ayant joué dans des films totalisant le plus de revenus.
       On suppose que le champ f.revenue est un float (ou 0.0) et représente le revenu d'un film.
       
    - La requête parcourt tous les films associés à un acteur, filtre ceux dont le revenu est défini et positif.
    - Elle somme le revenu pour chaque acteur, trie par revenu décroissant et retourne le top acteur.
    """
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    WHERE f.revenue IS NOT NULL AND f.revenue > 0
    RETURN a.name AS actor, SUM(f.revenue) AS totalRevenue
    ORDER BY totalRevenue DESC
    LIMIT 1
    """
    with driver.session() as session:
        record = session.run(query).single()
        if record:
            return record["actor"], record["totalRevenue"]
        else:
            return "Aucun acteur trouvé", 0.0

########################
# Q17
########################
def q17_avg_votes(driver):
    """
    17. Calcule la moyenne des votes des films.
       On suppose que le champ f.votes est un entier (ou 0).
       
    - La requête cible tous les films ayant un nombre de votes positif.
    - Elle calcule la moyenne de ces votes et retourne le résultat arrondi à deux décimales.
    """
    query = """
    MATCH (f:Film)
    WHERE f.votes IS NOT NULL AND f.votes > 0
    RETURN avg(f.votes) AS avgVotes
    """
    with driver.session() as session:
        record = session.run(query).single()
        if record and record["avgVotes"] is not None:
            return round(record["avgVotes"], 2)
        else:
            return None

########################
# Q19
########################
def q19_my_collaborative_films(driver: Driver):
    """
    19. Films collaboratifs - Version corrigée avec les bons noms.
       
    - La requête identifie d'abord les films dans lesquels les membres du projet (ici "Fenosoa" et "Mariam") ont joué.
    - Elle trouve ensuite les co-acteurs de ces films qui ne font pas partie de l'équipe projet.
    - Pour ces co-acteurs, la requête récupère leurs autres films qui n'impliquent pas de membres du projet.
    - Le résultat est une liste distincte des titres de films recommandés.
    """
    query = """
    // Trouver les films des membres du projet
    MATCH (target:Actor)-[:A_JOUE]->(targetFilm:Film)
    WHERE target.name IN ["Fenosoa", "Mariam"]
    
    // Trouver les co-acteurs dans ces films (hors membres du projet)
    MATCH (coActor:Actor)-[:A_JOUE]->(targetFilm)
    WHERE NOT coActor.name IN ["Fenosoa", "Coéquipier1", "Coéquipier2", "Mariam"]
    
    // Trouver leurs autres films qui ne contiennent PAS les membres du projet
    MATCH (coActor)-[:A_JOUE]->(recommendedFilm:Film)
    WHERE NOT EXISTS {
        MATCH (m:Actor)-[:A_JOUE]->(recommendedFilm)
        WHERE m.name IN ["Fenosoa", "Mariam"]
    }
    
    RETURN DISTINCT recommendedFilm.title AS film
    LIMIT 20
    """
    
    results = []
    with driver.session() as session:
        for record in session.run(query):
            results.append(record["film"])
    return results

########################
# Q20
########################
def q20_director_more_actors(driver: Driver):
    """
    20. Quel réalisateur a travaillé avec le plus grand nombre d’acteurs distincts ?
       
    - La requête parcourt les films réalisés par chaque réalisateur et identifie tous les acteurs ayant joué dans ces films.
    - Elle compte le nombre d'acteurs distincts pour chaque réalisateur, trie par ce nombre, et retourne le meilleur résultat.
    """
    query = """
    MATCH (r:Realisateur)-[:REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    RETURN r.name AS director, COUNT(DISTINCT a) AS nbActors
    ORDER BY nbActors DESC
    LIMIT 1
    """
    with driver.session() as session:
        record = session.run(query).single()
        if record:
            return record["director"], record["nbActors"]
        return None, 0

########################
# Q21
########################
def q21_most_connected_films(driver, limit=5):
    """
    21. Quels sont les films "les plus connectés" ?
        i.e. ceux qui ont le plus d'acteurs en commun avec d'autres films.
       
    - La requête recherche pour chaque film le nombre d'acteurs qui jouent également dans d'autres films.
    - Elle agrège ces connexions pour attribuer un score de "connectivité" à chaque film.
    - Les films sont triés par ce score en ordre décroissant et le nombre de résultats est limité.
    """
    query = f"""
    MATCH (f1:Film)<-[:A_JOUE]-(a:Actor)-[:A_JOUE]->(f2:Film)
    WHERE f1 <> f2
    WITH f1, f2, COUNT(DISTINCT a) AS nbCommon
    WITH f1, SUM(nbCommon) AS totalCommon
    RETURN f1.title AS film, totalCommon
    ORDER BY totalCommon DESC
    LIMIT {limit}
    """
    results = []
    with driver.session() as session:
        for record in session.run(query):
            results.append((record["film"], record["totalCommon"]))
    return results

########################
# Q22
########################
def q22_actors_most_directors(driver, limit=5):
    """
    22. Trouver les acteurs ayant joué avec le plus de réalisateurs différents.
       Retourne les top N acteurs sous la forme d'un tuple (nom, nbDirectors).
       
    - La requête cherche les films dans lesquels un acteur a joué et récupère les réalisateurs correspondants.
    - Elle compte ensuite le nombre de réalisateurs distincts pour chaque acteur, trie les résultats et limite l'affichage.
    """
    query = f"""
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:REALISE]-(r:Realisateur)
    RETURN a.name AS actor, COUNT(DISTINCT r) AS nbDirectors
    ORDER BY nbDirectors DESC
    LIMIT {limit}
    """
    results = []
    with driver.session() as session:
        for record in session.run(query):
            results.append((record["actor"], record["nbDirectors"]))
    return results

########################
# Q23
########################
def q23_recommend_film_by_genre(driver, actor_name="John Boyega"):
    """
    23. Recommander un film en fonction du genre dominant de l'acteur (version corrigée).
       
    - La première partie de la requête identifie le genre le plus fréquent parmi les films dans lesquels l'acteur a joué.
    - La seconde partie cherche un film dans ce genre que l'acteur n'a pas encore joué, en sélectionnant un film de façon aléatoire.
    """
    query = """
    // Trouver le genre le plus fréquent de l'acteur
    MATCH (a:Actor {name: $actorName})-[:A_JOUE]->(f:Film)-[:A_POUR_GENRE]->(g:Genre)
    WITH a, g, COUNT(g) AS genreCount
    ORDER BY genreCount DESC
    LIMIT 1
    
    // Trouver un film aléatoire de ce genre non joué par l'acteur
    MATCH (g)<-[:A_POUR_GENRE]-(recFilm:Film)
    WHERE NOT EXISTS {
        MATCH (a)-[:A_JOUE]->(recFilm)
    }
    RETURN recFilm.title AS recommendedFilm
    ORDER BY rand()
    LIMIT 1
    """
    
    with driver.session() as session:
        result = session.run(query, actorName=actor_name)
        record = result.single()
        return record["recommendedFilm"] if record else "Aucune recommandation trouvée"

########################
# Q24
########################
def q24_influence_by_genre(driver, min_shared_genres=2):
    """
    24. Analyser l'influence entre réalisateurs basée sur le nombre de genres qu'ils partagent.
       
    - La requête collecte les genres uniques associés à chaque réalisateur.
    - Elle calcule ensuite l'intersection des genres entre paires de réalisateurs.
    - Si le nombre de genres communs dépasse un seuil (min_shared_genres), une relation d'influence est créée.
    - Le résultat renvoie les paires de réalisateurs et le nombre de genres qu'ils partagent.
    """
    query = """
    // Collecter les genres uniques pour chaque réalisateur
    MATCH (r1:Realisateur)-[:REALISE]->(f1:Film)-[:A_POUR_GENRE]->(g1:Genre)
    WITH r1, COLLECT(DISTINCT g1.name) AS genres_r1

    MATCH (r2:Realisateur)-[:REALISE]->(f2:Film)-[:A_POUR_GENRE]->(g2:Genre)
    WITH r1, genres_r1, r2, COLLECT(DISTINCT g2.name) AS genres_r2

    WHERE r1 <> r2

    // Calculer l'intersection des genres
    WITH r1, r2, 
         [genre IN genres_r1 WHERE genre IN genres_r2] AS commonGenres

    WHERE SIZE(commonGenres) >= $min_shared

    // Créer la relation d'influence entre réalisateurs
    MERGE (r1)-[:INFLUENCE_PAR {
        commonGenres: SIZE(commonGenres)
    }]->(r2)

    RETURN r1.name AS real1, 
           r2.name AS real2, 
           SIZE(commonGenres) AS sharedGenres
    ORDER BY sharedGenres DESC
    """
    
    results = []
    with driver.session() as session:
        for record in session.run(query, min_shared=min_shared_genres):
            results.append((record["real1"], record["real2"], record["sharedGenres"]))
    return results

########################
# Q25
########################
def q25_shortest_path_between_actors(driver, actor1="Mark Strong", actor2="Will Smith"):
    """
    25. Retourne la liste des noms du plus court chemin entre 2 acteurs.
       
    - La requête utilise la fonction shortestPath pour trouver le chemin le plus court entre les deux acteurs.
    - Le chemin est ensuite parcouru pour extraire les noms des nœuds (soit les acteurs, soit les films).
    - La fonction retourne une liste de noms pour une flexibilité d'affichage.
    """
    query = """
    MATCH p = shortestPath(
        (a1:Actor {name:$actor1})-[*]-(a2:Actor {name:$actor2})
    )
    RETURN p
    """
    with driver.session() as session:
        record = session.run(query, actor1=actor1, actor2=actor2).single()
        if not record or not record["p"]:
            return None
        
        path = record["p"]  # Objet Path représentant le chemin trouvé
        node_names = []
        # Parcours de chaque nœud du chemin pour extraire le nom (ou le titre pour un film)
        for node in path.nodes:
            node_names.append(node.get("name") or node.get("title") or "???")
        
        return node_names

########################
# Q26
########################
def q26_actors_communities(driver):
    """
    26. Analyser les communautés d'acteurs via l'algorithme Louvain de GDS.
       
    - La première étape projette le graphe en utilisant une requête Cypher,
      en sélectionnant uniquement les acteurs et leurs connexions via les films.
    - Ensuite, l'algorithme Louvain est appliqué pour identifier les communautés.
    - Les communautés sont ensuite récupérées pour chaque acteur.
    - Enfin, le graphe projeté est supprimé pour libérer les ressources.
    """
    # 1. Projection du graphe : création d'un graphe temporaire "actorGraph"
    project_query = """
    CALL gds.graph.project.cypher(
      'actorGraph',
      'MATCH (a:Actor) RETURN id(a) AS id, labels(a) AS labels',
      'MATCH (a1:Actor)-[:A_JOUE]->(:Film)<-[:A_JOUE]-(a2:Actor)
       WHERE a1 <> a2
       RETURN id(a1) AS source, id(a2) AS target'
    )
    YIELD graphName
    """

    # 2. Application de l'algorithme Louvain pour détecter les communautés
    louvain_query = """
    CALL gds.louvain.write(
      'actorGraph',
      { writeProperty: 'communityId' }
    )
    YIELD communityCount, modularity
    """

    # 3. Récupération des acteurs avec leur identifiant de communauté
    result_query = """
    MATCH (a:Actor)
    RETURN a.name AS actor, a.communityId AS community
    ORDER BY community, actor
    """

    # 4. Suppression du graphe temporaire pour libérer la mémoire
    drop_query = "CALL gds.graph.drop('actorGraph') YIELD graphName"

    results = []
    with driver.session() as session:
        session.run(project_query)
        session.run(louvain_query)
        res = session.run(result_query)
        for record in res:
            results.append((record["actor"], record["community"]))
        session.run(drop_query)
    return results

########################
# Q27
########################
def q27_same_genre_diff_directors(driver, limit=10):
    """
    27. Quels sont les films qui ont des genres en commun mais ont des réalisateurs différents ?
       
    - La requête identifie des paires de films qui partagent le même genre via la relation A_POUR_GENRE.
    - Elle s'assure ensuite que les films proviennent de réalisateurs différents.
    - Les résultats sont triés et limités par le paramètre limit.
    """
    query = f"""
    MATCH (r1:Realisateur)-[:REALISE]->(f1:Film)-[:A_POUR_GENRE]->(g:Genre)<-[:A_POUR_GENRE]-(f2:Film)<-[:REALISE]-(r2:Realisateur)
    WHERE f1 <> f2
      AND r1 <> r2
    RETURN f1.title AS film1,
           r1.name  AS director1,
           f2.title AS film2,
           r2.name  AS director2,
           g.name   AS sharedGenre
    ORDER BY film1, film2
    LIMIT {limit}
    """
    results = []
    with driver.session() as session:
        for record in session.run(query):
            results.append((
                record["film1"],
                record["director1"],
                record["film2"],
                record["director2"],
                record["sharedGenre"],
            ))
    return results

########################
# Q28
########################
def q28_recommend_films_for_user(driver, user_name="Alice", limit=5):
    """
    28. Recommander des films à un utilisateur en fonction des préférences d'un acteur donné.
       
    - La requête cherche un utilisateur (User) qui aime un acteur particulier.
    - Elle récupère les films dans lesquels cet acteur apparaît, en excluant ceux déjà visionnés par l'utilisateur.
    - Les films sont retournés de manière aléatoire avec une limite de résultats.
    """
    query = f"""
    MATCH (u:User {{name:$userName}})-[:LIKE_ACTOR]->(a:Actor)-[:A_JOUE]->(f:Film)
    WHERE NOT (u)-[:HAS_WATCHED]->(f)
    RETURN DISTINCT f.title AS recommendedFilm
    ORDER BY rand()
    LIMIT {limit}
    """
    results = []
    with driver.session() as session:
        for record in session.run(query, userName=user_name):
            results.append(record["recommendedFilm"])
    return results

########################
# Q29
########################
def q29_create_concurrence_relationship(driver):
    """
    29. Créer une relation 'CONCURRENT' entre réalisateurs ayant réalisé des films similaires
        la même année et dans le même genre.
       
    - La requête trouve deux réalisateurs (r1 et r2) qui ont réalisé des films appartenant au même genre (g)
      dans la même année.
    - Une relation CONCURRENT est créée entre ces réalisateurs avec des propriétés (année, genre).
    - Les résultats retournent les paires de réalisateurs et les détails de leur concurrence.
    """
    query = """
    MATCH (r1:Realisateur)-[:REALISE]->(f1:Film)-[:A_POUR_GENRE]->(g:Genre),
          (r2:Realisateur)-[:REALISE]->(f2:Film)-[:A_POUR_GENRE]->(g)
    WHERE r1 <> r2
      AND f1.year IS NOT NULL
      AND f2.year = f1.year
    MERGE (r1)-[c:CONCURRENT {year: f1.year, genre: g.name}]->(r2)
    RETURN r1.name AS real1, r2.name AS real2, f1.year AS sameYear, g.name AS sameGenre
    ORDER BY real1, real2
    """
    results = []
    with driver.session() as session:
        for record in session.run(query):
            results.append((
                record["real1"],
                record["real2"],
                record["sameYear"],
                record["sameGenre"]
            ))
    return results

########################
# Q30
########################
def q30_top_director_actor_collabs(driver, limit=10):
    """
    30. Identifier les collaborations les plus fréquentes entre réalisateurs et acteurs,
        et analyser leur succès commercial via les revenus cumulés.
       
    - La requête parcourt les films, compte le nombre de collaborations entre chaque réalisateur et acteur,
      et somme les revenus des films associés.
    - Les résultats sont triés d'abord par le nombre de collaborations puis par les revenus, et limités au paramètre limit.
    """
    query = """
    MATCH (r:Realisateur)-[:REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    WHERE f.revenue IS NOT NULL AND f.revenue > 0
    WITH r, a, 
         COUNT(f) AS collaborations,
         SUM(f.revenue) AS totalRevenue
    ORDER BY collaborations DESC, totalRevenue DESC
    LIMIT $limit
    RETURN r.name AS director,
           a.name AS actor,
           collaborations,
           totalRevenue
    """
    
    results = []
    with driver.session() as session:
        for record in session.run(query, limit=limit):
            results.append((
                record["director"],     # Nom du réalisateur
                record["actor"],        # Nom de l'acteur
                record["collaborations"], # Nombre de collaborations
                round(record["totalRevenue"], 2)  # Revenu total arrondi
            ))
    return results
