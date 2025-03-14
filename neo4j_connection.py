from config import get_neo4j_driver

def test_connection():
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            result = session.run("RETURN 'Connexion réussie à Neo4j' AS message")
            for record in result:
                print(record["message"])
    except Exception as e:
        print(f"Erreur de connexion : {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    test_connection()
