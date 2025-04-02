import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------
# Imports pour les requêtes MongoDB (Questions 1 à 13)
# ------------------------------
from queries import (
    q1_most_movies_year,
    q2_films_after_1999,
    q3_avg_votes_2007,
    q4_histogram_films_by_year,
    q5_available_genres,
    q6_max_revenue_film,
    q7_directors_more_than_5,
    q8_genre_highest_avg_revenue,
    q9_top_3_rated_per_decade,
    q10_longest_film_by_genre,
    q11_create_view_high_quality,
    q12_runtime_revenue_correlation,
    q12_runtime_revenue_data,
    q13_evolution_avg_runtime_per_decade,
    q18_genre_most_represented_mongo
)

# ------------------------------
# Imports pour les requêtes Neo4j (Questions 14 à 30)
# ------------------------------
from queries_neo4j import (
    q14_actor_most_films,
    q15_actor_most_films_with_anne_hathaway,
    q16_actor_most_revenue,
    q17_avg_votes,
    q19_my_collaborative_films,
    q20_director_more_actors,
    q21_most_connected_films,
    q22_actors_most_directors,
    q23_recommend_film_by_genre,
    q24_influence_by_genre,
    q25_shortest_path_between_actors,
    q26_actors_communities,
    q27_same_genre_diff_directors,
    q28_recommend_films_for_user,
    q29_create_concurrence_relationship,
    q30_top_director_actor_collabs
)

def make_dot_path(node_names):
    """
    Construit une chaîne de caractères au format DOT pour visualiser un chemin dans le graphe.
    Ce format est utilisé par Graphviz pour représenter des graphes.
    """
    dot = "digraph G {\n"
    dot += "  rankdir=LR;\n"  # Orientation du graphe de gauche à droite
    for i in range(len(node_names) - 1):
        dot += f'  "{node_names[i]}" -> "{node_names[i+1]}";\n'
    dot += "}"
    return dot

# ------------------------------
# Connexion à Neo4j
# ------------------------------
from neo4j import GraphDatabase
# Création d'une instance de driver pour se connecter à Neo4j via le protocole Bolt
driver_neo4j = GraphDatabase.driver(
    "bolt://54.210.151.222:7687",  # URL de l'instance Neo4j
    auth=("neo4j", "notice-rates-definitions")  # Identifiants de connexion
)

def main():
    # Titre et introduction de l'application dans l'interface Streamlit
    st.title("Application d'Analyses NoSQL – MongoDB & Neo4j")
    st.markdown("""
    Cette application interroge **MongoDB** (Q1 à Q13) et **Neo4j** (Q14 à Q30).
    Cliquez sur les boutons pour afficher les résultats et les visualisations.
    ---
    """)

    # ====================================================
    # SECTION : Requêtes MongoDB (Questions 1 à 13)
    # ====================================================
    st.header("Partie MongoDB (Q1 à Q13)")

    # Q1 : Année avec le plus grand nombre de films
    st.subheader("Q1. Année avec le plus grand nombre de films")
    if st.button("Afficher Q1"):
        year, count = q1_most_movies_year()
        if year:
            st.success(f"L'année **{year}** possède **{count}** films.")
        else:
            st.warning("Pas de résultat.")

    # Q2 : Nombre de films sortis après 1999
    st.subheader("Q2. Films sortis après 1999")
    if st.button("Afficher Q2"):
        count = q2_films_after_1999()
        st.info(f"Nombre de films après 1999 : **{count}**")

    # Q3 : Moyenne des votes pour les films de 2007
    st.subheader("Q3. Moyenne des votes (2007)")
    if st.button("Afficher Q3"):
        avg_votes = q3_avg_votes_2007()
        st.info(f"Moyenne des votes en 2007 : **{avg_votes}**")

    # Q4 : Histogramme du nombre de films par année
    st.subheader("Q4. Histogramme des films par année")
    if st.button("Afficher Q4"):
        fig = q4_histogram_films_by_year()
        st.pyplot(fig)

    # Q5 : Liste des genres disponibles dans la base
    st.subheader("Q5. Genres disponibles")
    if st.button("Afficher Q5"):
        genres = q5_available_genres()
        if genres:
            st.write("**Genres disponibles :**", genres)
        else:
            st.warning("Aucun genre trouvé.")

    # Q6 : Film générant le plus de revenu
    st.subheader("Q6. Film avec le plus de revenu")
    if st.button("Afficher Q6"):
        film, revenue = q6_max_revenue_film()
        if film:
            st.success(f"**{film}** a généré **{revenue}** M$.")
        else:
            st.warning("Pas de résultat.")

    # Q7 : Réalisateurs ayant réalisé plus de 5 films
    st.subheader("Q7. Réalisateurs ayant réalisé plus de 5 films")
    if st.button("Afficher Q7"):
        data = q7_directors_more_than_5()
        if data:
            st.write("**Réalisateurs avec > 5 films :**")
            for d in data:
                st.write(f"- {d['_id']}: {d['count']} films")
        else:
            st.warning("Aucun réalisateur trouvé.")

    # Q8 : Genre avec le revenu moyen le plus élevé
    st.subheader("Q8. Genre au revenu moyen le plus élevé")
    if st.button("Afficher Q8"):
        genre, avg_rev = q8_genre_highest_avg_revenue()
        if genre:
            st.success(f"Le genre **{genre}** a un revenu moyen de **{avg_rev}** M$.")
        else:
            st.warning("Pas de résultat.")

    # Q9 : Top 3 films par décennie basés sur le Metascore
    st.subheader("Q9. Top 3 films par décennie (Metascore)")
    if st.button("Afficher Q9"):
        data = q9_top_3_rated_per_decade()
        if data:
            for decade_info in data:
                dec = decade_info["_id"]
                top3 = decade_info["top3"]
                st.write(f"### Décennie : {dec}")
                for film in top3:
                    title = film.get("title", "N/A")
                    metascore = film.get("Metascore", "N/A")
                    st.write(f"- **{title}** (Metascore : {metascore})")
        else:
            st.warning("Aucun résultat.")

    # Q10 : Film le plus long par genre, affiché en texte et en graphique
    st.subheader("Q10. Film le plus long par genre")
    col10a, col10b = st.columns(2)
    with col10a:
        if st.button("Afficher Q10 (texte)"):
            data = q10_longest_film_by_genre()
            if data:
                st.write("**Films les plus longs par genre :**")
                for item in data:
                    genre_id = item["_id"]
                    film = item["film"]
                    if film:
                        title = film.get("title", "N/A")
                        runtime = film.get("Runtime (Minutes)", "N/A")
                        st.write(f"- **{genre_id}** : {title} ({runtime} min)")
            else:
                st.warning("Pas de résultat.")
    with col10b:
        if st.button("Afficher Q10 (graphique)"):
            data = q10_longest_film_by_genre()
            if data:
                items = []
                for item in data:
                    genre_id = item["_id"]
                    film = item["film"]
                    if film:
                        runtime = film.get("Runtime (Minutes)")
                        if runtime is not None:
                            try:
                                runtime = float(runtime)
                                items.append((genre_id, runtime))
                            except:
                                continue
                if items:
                    import pandas as pd
                    df = pd.DataFrame(items, columns=["genre", "runtime"])
                    fig, ax = plt.subplots(figsize=(8, 4))
                    sns.barplot(data=df, x="genre", y="runtime", ax=ax)
                    ax.set_xlabel("Genre")
                    ax.set_ylabel("Runtime (Minutes)")
                    ax.set_title("Film le plus long par genre")
                    plt.xticks(rotation=45)
                    st.pyplot(fig)
                else:
                    st.warning("Aucun résultat graphique.")
            else:
                st.warning("Pas de résultat.")

    # Q11 : Création d'une vue MongoDB filtrant les films de haute qualité
    st.subheader("Q11. Créer une vue MongoDB (Metascore > 80, Revenu > 50M$)")
    if st.button("Créer Q11"):
        msg = q11_create_view_high_quality()
        st.info(msg)

    # Q12 : Corrélation entre la durée et le revenu des films
    st.subheader("Q12. Corrélation entre durée et revenu")
    if st.button("Afficher Q12"):
        corr, p_value = q12_runtime_revenue_correlation()
        st.write(f"**Corrélation de Pearson** : {corr:.3f}, **p-value** : {p_value:.3g}")
        df = q12_runtime_revenue_data()
        if len(df) < 2:
            st.warning("Pas assez de données pour un graphique.")
        else:
            fig, ax = plt.subplots()
            sns.regplot(x="Runtime (Minutes)", y="Revenue (Millions)", data=df, ax=ax)
            ax.set_xlabel("Durée (min)")
            ax.set_ylabel("Revenu (M$)")
            ax.set_title("Corrélation entre durée et revenu")
            st.pyplot(fig)

    # Q13 : Évolution de la durée moyenne des films par décennie (texte et graphique)
    st.subheader("Q13. Évolution de la durée moyenne des films par décennie")
    col13a, col13b = st.columns(2)
    with col13a:
        if st.button("Afficher Q13 (texte)"):
            data = q13_evolution_avg_runtime_per_decade()
            if data:
                st.write("**Durée moyenne par décennie :**")
                for item in data:
                    dec = item["_id"]
                    avg_rt = item["avg_runtime"]
                    st.write(f"- Décennie {dec} : {avg_rt} min")
            else:
                st.warning("Aucun résultat.")
    with col13b:
        if st.button("Afficher Q13 (graphique)"):
            data = q13_evolution_avg_runtime_per_decade()
            if data:
                data_filtered = [d for d in data if d["_id"] is not None and d["avg_runtime"] is not None]
                if not data_filtered:
                    st.warning("Aucun résultat.")
                else:
                    decades = [d["_id"] for d in data_filtered]
                    avg_runtimes = [d["avg_runtime"] for d in data_filtered]
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.plot(decades, avg_runtimes, marker='o', linestyle='-')
                    ax.set_xlabel("Décennie")
                    ax.set_ylabel("Durée moyenne (min)")
                    ax.set_title("Évolution de la durée moyenne")
                    st.pyplot(fig)
            else:
                st.warning("Aucun résultat.")

    # ====================================================
    # SECTION : Requêtes Neo4j (Questions 14 à 30)
    # ====================================================
    st.header("Partie Neo4j (Questions 14 à 30)")
    st.markdown("*Interrogation du graphe Neo4j*")

    # Q14 : Acteur ayant joué dans le plus de films
    if st.button("Q14 - Acteur le plus prolifique"):
        actor, nbFilms = q14_actor_most_films(driver_neo4j)
        if actor:
            st.success(f"L'acteur **{actor}** a joué dans **{nbFilms}** films.")
        else:
            st.warning("Pas de résultat.")

    # Q15 : Acteur avec le plus de films en commun avec Anne Hathaway
    if st.button("Q15 - Films communs avec Anne Hathaway"):
        actor, nbCommon = q15_actor_most_films_with_anne_hathaway(driver_neo4j)
        if actor:
            st.info(f"**{actor}** a joué avec Anne Hathaway dans **{nbCommon}** films.")
        else:
            st.warning("Pas de résultat.")

    # Q16 : Acteur ayant généré le plus de revenus cumulés
    if st.button("Q16 - Acteur le plus rentable"):
        actor, totalRev = q16_actor_most_revenue(driver_neo4j)
        if actor:
            st.success(f"**{actor}** a généré un total de **{totalRev}** en revenus.")
        else:
            st.warning("Pas de résultat.")

    # Q17 : Moyenne des votes pour tous les films
    if st.button("Q17 - Moyenne des votes"):
        avg = q17_avg_votes(driver_neo4j)
        if avg:
            st.success(f"Moyenne des votes sur tous les films : **{avg:.2f}**")
        else:
            st.warning("Pas de résultat.")

    # Q18 : Genre le plus représenté (MongoDB)
    if st.button("Q18 - Genre le plus représenté"):
        try:
            genre, count = q18_genre_most_represented_mongo()
            if genre:
                st.success(f"Genre le plus populaire : **{genre}** ({count} films)")
            else:
                st.warning("Aucun genre trouvé dans la base de données")
        except Exception as e:
            st.error(f"Erreur lors de la récupération des données : {str(e)}")

    # Q19 : Films collaboratifs
    if st.button("Q19 - Films collaboratifs"):
        films = q19_my_collaborative_films(driver_neo4j)
        if films:
            st.write("**Films collaboratifs trouvés :**")
            for film in films:
                st.write(f"- {film}")
        else:
            st.warning("Aucun film collaboratif trouvé. Vérifiez les données.")

    # Q20 : Réalisateur ayant travaillé avec le plus d'acteurs distincts
    if st.button("Q20 - Réalisateur le plus collaboratif"):
        real, nb = q20_director_more_actors(driver_neo4j)
        if real:
            st.success(f"**{real}** a travaillé avec **{nb}** acteurs différents.")
        else:
            st.warning("Pas de résultat.")

    # Q21 : Films les plus connectés
    if st.button("Q21 - Films les plus connectés"):
        films = q21_most_connected_films(driver_neo4j)
        if films:
            st.write("**Top 5 films connectés** :")
            for film, score in films:
                st.write(f"- {film} ({score} connexions)")
        else:
            st.warning("Pas de résultat.")

    # Q22 : Acteurs ayant joué avec le plus de réalisateurs différents
    if st.button("Q22 - Acteurs polyvalents"):
        actors = q22_actors_most_directors(driver_neo4j)
        if actors:
            st.write("**Top 5 acteurs** :")
            for actor, nb in actors:
                st.write(f"- {actor} ({nb} réalisateurs)")
        else:
            st.warning("Pas de résultat.")

    # Q23 : Recommandation de film basée sur le genre dominant d'un acteur
    st.subheader("Q23 - Recommandation de film")
    actor_name = st.text_input("Nom de l'acteur", "John Boyega")
    if st.button("Générer recommandation"):
        film = q23_recommend_film_by_genre(driver_neo4j, actor_name)
        if film:
            st.success(f"Recommandation pour {actor_name} : **{film}**")
        else:
            st.warning("Pas de recommandation trouvée")

    # Q24 : Analyse des relations d'influence entre réalisateurs
    if st.button("Q24 - Influence entre réalisateurs"):
        influences = q24_influence_by_genre(driver_neo4j)
        if influences:
            st.write("**Relations d'influence** :")
            for r1, r2, nb in influences[:20]:
                st.write(f"- {r1} → {r2} ({nb} genres communs)")
        else:
            st.warning("Pas de résultat.")

    # Q25 : Chemin le plus court entre deux acteurs (visualisé via Graphviz)
    if st.button("Q25 - Chemin le plus court (Mark Strong - Will Smith)"):
        path_list = q25_shortest_path_between_actors(driver_neo4j, "Mark Strong", "Will Smith")
        if path_list is None:
            st.warning("Aucun chemin trouvé.")
        else:
            dot_code = make_dot_path(path_list)
            st.graphviz_chart(dot_code)

    # Q26 : Analyse des communautés d'acteurs via l'algorithme Louvain
    if st.button("Q26 - Communautés d'acteurs"):
        comms = q26_actors_communities(driver_neo4j)
        if comms:
            st.write("**Membres des communautés** :")
            for actor, cid in comms[:50]:
                st.write(f"- {actor} (communauté {cid})")
        else:
            st.warning("Pas de résultat")

    # Q27 : Films de même genre réalisés par des réalisateurs différents
    if st.button("Q27 - Films similaires"):
        films = q27_same_genre_diff_directors(driver_neo4j)
        if films:
            st.write("**Paires de films similaires** :")
            for f1, d1, f2, d2, g in films:
                st.write(f"- {f1} ({d1}) et {f2} ({d2}) - Genre: {g}")
        else:
            st.warning("Pas de résultat")

    # Q28 : Recommandations personnalisées pour un utilisateur donné (ex: Bob)
    if st.button("Q28 - Recommandations personnalisées pour Bob"):
        recs = q28_recommend_films_for_user(driver_neo4j, user_name="Bob", limit=5)
        if recs:
            st.write("**Films recommandés pour Bob** :")
            for film in recs:
                st.write(f"- {film}")
        else:
            st.warning("Pas de recommandations pour Bob")

    # Q29 : Création d'une relation 'CONCURRENT' entre réalisateurs ayant des films similaires
    if st.button("Q29 - Relations de concurrence"):
        concur = q29_create_concurrence_relationship(driver_neo4j)
        if concur:
            st.write("**Relations créées** :")
            for r1, r2, y, g in concur[:20]:
                st.write(f"- {r1} ↔ {r2} ({y}, {g})")
        else:
            st.warning("Pas de résultat")

    # Q30 : Identifier les meilleures collaborations entre réalisateurs et acteurs et analyser le revenu
    if st.button("Q30 - Top collaborations"):
        collabs = q30_top_director_actor_collabs(driver_neo4j)
        if collabs:
            st.write("**Meilleures collaborations** :")
            for director, actor, nb_films, total_revenue in collabs:
                st.write(f"- {director} & {actor}: {nb_films} films")
                st.write(f"  Revenu total: ${total_revenue:.2f}M")
        else:
            st.warning("Pas de résultat")

    # Fermeture de la connexion au driver Neo4j pour libérer les ressources
    driver_neo4j.close()

if __name__ == "__main__":
    main()
