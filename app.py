import streamlit as st
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
    q13_evolution_avg_runtime_per_decade
)

def main():
    st.title("Questions MongoDB – Analyses de Films")

    # ========== Q1 ==========
    st.subheader("1. Année avec le plus grand nombre de films")
    if st.button("Afficher la réponse (Q1)"):
        year, count = q1_most_movies_year()
        if year:
            st.write(f"L'année {year} a le plus grand nombre de films ({count}).")
        else:
            st.write("Pas de résultat.")

    # ========== Q2 ==========
    st.subheader("2. Nombre de films sortis après 1999")
    if st.button("Afficher la réponse (Q2)"):
        count = q2_films_after_1999()
        st.write(f"{count} films après 1999.")

    # ========== Q3 ==========
    st.subheader("3. Moyenne des votes (films sortis en 2007)")
    if st.button("Afficher la réponse (Q3)"):
        avg_votes = q3_avg_votes_2007()
        st.write(f"Moyenne des votes en 2007 : {avg_votes}")

    # ========== Q4 ==========
    st.subheader("4. Histogramme des films par année")
    if st.button("Afficher l'histogramme (Q4)"):
        fig = q4_histogram_films_by_year()
        st.pyplot(fig)

    # ========== Q5 ==========
    st.subheader("5. Genres de films disponibles dans la base")
    if st.button("Afficher la réponse (Q5)"):
        genres = q5_available_genres()
        st.write("Genres disponibles :", genres)

    # ========== Q6 ==========
    st.subheader("6. Film qui a généré le plus de revenu")
    if st.button("Afficher la réponse (Q6)"):
        film, revenue = q6_max_revenue_film()
        st.write(f"Le film '{film}' a généré {revenue} millions de dollars.")

    # ========== Q7 ==========
    st.subheader("7. Réalisateurs ayant réalisé plus de 5 films")
    if st.button("Afficher la réponse (Q7)"):
        data = q7_directors_more_than_5()
        if data:
            st.write("Liste des réalisateurs ayant > 5 films :")
            for d in data:
                st.write(f"- {d['_id']} : {d['count']} films")
        else:
            st.write("Aucun réalisateur avec plus de 5 films.")

    # ========== Q8 ==========
    st.subheader("8. Genre de film qui rapporte en moyenne le plus de revenus")
    if st.button("Afficher la réponse (Q8)"):
        genre, avg_rev = q8_genre_highest_avg_revenue()
        if genre:
            st.write(f"Le genre '{genre}' a le revenu moyen le plus élevé : {avg_rev} M$")
        else:
            st.write("Pas de résultat.")

    # ========== Q9 ==========
    st.subheader("9. Top 3 films les mieux notés par décennie")
    if st.button("Afficher la réponse (Q9)"):
        data = q9_top_3_rated_per_decade()
        if data:
            for decade_info in data:
                decade = decade_info["_id"]
                top3 = decade_info["top3"]
                st.write(f"Décennie : {decade}")
                for film in top3:
                    st.write(f" - {film['title']} (rating : {film['rating']})")
        else:
            st.write("Aucun résultat.")

    # ========== Q10 ==========
    st.subheader("10. Film le plus long (Runtime) par genre")
    if st.button("Afficher la réponse (Q10)"):
        data = q10_longest_film_by_genre()
        if data:
            st.write("Film le plus long par genre :")
            for item in data:
                genre_id = item["_id"]
                film = item["film"]
                if film:
                    title = film.get("title", "N/A")
                    runtime = film.get("Runtime (Minutes)", "N/A")
                    st.write(f"- Genre : {genre_id} => '{title}', durée : {runtime} min")
        else:
            st.write("Pas de résultat.")

    # ========== Q11 ==========
    st.subheader("11. Créer une vue MongoDB (films note > 80, revenu > 50M$)")
    if st.button("Créer la vue (Q11)"):
        msg = q11_create_view_high_quality()
        st.write(msg)

    # ========== Q12 ==========
    st.subheader("12. Corrélation entre durée (Runtime) et revenu (Revenue)")
    if st.button("Afficher la corrélation (Q12)"):
        corr, p_value = q12_runtime_revenue_correlation()
        st.write(f"Corrélation de Pearson : {corr}, p-value : {p_value}")

    # ========== Q13 ==========
    st.subheader("13. Évolution de la durée moyenne des films par décennie")
    if st.button("Afficher la réponse (Q13)"):
        data = q13_evolution_avg_runtime_per_decade()
        if data:
            st.write("Durée moyenne par décennie :")
            for item in data:
                dec = item["_id"]
                avg_rt = item["avg_runtime"]
                st.write(f"- Décennie {dec} => {avg_rt} minutes")
        else:
            st.write("Pas de résultat.")

if __name__ == "__main__":
    main()
