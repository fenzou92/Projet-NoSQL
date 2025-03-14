from pymongo import MongoClient
from neo4j import GraphDatabase

# Configuration de la connexion à MongoDB
MONGO_URI = "mongodb://127.0.0.1:27017/"
DATABASE_NAME = "entertainment"
COLLECTION_NAME = "films"

# Configuration de la connexion à Neo4j
NEO4J_URI = "bolt://52.205.250.203:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "sign-talks-recombinations"

# Fonction pour obtenir la base de données MongoDB
def get_database():
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]

# Fonction pour obtenir le driver Neo4j
def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
