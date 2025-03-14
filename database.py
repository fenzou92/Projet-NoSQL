from pymongo import MongoClient
import config

def get_mongo_collection():
    client = MongoClient(config.MONGO_URI)
    db = client[config.DATABASE_NAME]
    return db[config.COLLECTION_NAME]
