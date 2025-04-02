from pymongo import MongoClient
from neo4j import GraphDatabase

# === Configuration MongoDB (Atlas) ===
MONGO_URI = "mongodb+srv://feno:u1hcslgM3AkFGOru@cluster0.l1dryix.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "entertainment"
COLLECTION_NAME = "films"

def get_database():
    """
    Retourne l'objet database de MongoDB.
    """
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]

# === Configuration Neo4j (Sandbox) ===
# Utilisation de l'URL Bolt classique
NEO4J_URI = "bolt://54.210.151.222:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "notice-rates-definitions"

def get_neo4j_driver():
    """
    Retourne un driver Neo4j pour établir une connexion.
    """
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
