import os
from pymongo import MongoClient
from datetime import datetime

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MONGO_DB_NAME", "seo_crawler_db")

def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def save_crawl(site, crawl):
    """
    Save a crawl snapshot. Documents are stored in collection `crawls`.
    """
    client = get_client()
    db = client[DB_NAME]
    col = db["crawls"]
    doc = {
        "site": site,
        "timestamp": datetime.utcnow(),
        "crawl": crawl
    }
    res = col.insert_one(doc)
    client.close()
    return res.inserted_id

def latest_crawl(site):
    client = get_client()
    db = client[DB_NAME]
    col = db["crawls"]
    doc = col.find_one({"site": site}, sort=[("timestamp", -1)])
    client.close()
    return doc

def list_crawls(site, limit=10):
    client = get_client()
    db = client[DB_NAME]
    col = db["crawls"]
    docs = list(col.find({"site": site}).sort("timestamp", -1).limit(limit))
    client.close()
    return docs

def delete_database():
    client = get_client()
    client.drop_database(DB_NAME)
    client.close()
    return True
