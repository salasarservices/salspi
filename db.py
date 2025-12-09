# db.py
import os
import logging
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger("site_crawler_db")
logger.setLevel(logging.INFO)

def _resolve_uri(uri: Optional[str]) -> Optional[str]:
    """
    Resolve which Mongo URI to use.
    Priority:
      1) explicit uri argument (if not empty)
      2) environment variable MONGO_URI
    Returns None if nothing found.
    """
    if uri:
        return uri
    env_uri = os.getenv("MONGO_URI")
    if env_uri:
        return env_uri
    return None

def get_mongo_client(uri: Optional[str], server_selection_timeout_ms: int = 5000) -> MongoClient:
    """
    Return a MongoClient for the provided URI. Raises ValueError if uri is None.
    Caller is responsible for closing client.
    """
    resolved = _resolve_uri(uri)
    if not resolved:
        raise ValueError("No MongoDB URI provided. Set uri argument or MONGO_URI env var.")
    return MongoClient(resolved, serverSelectionTimeoutMS=server_selection_timeout_ms)

def save_pages_to_mongo(pages: List[Dict[str, Any]],
                        uri: Optional[str] = None,
                        db_name: str = "sitecrawler",
                        collection_name: str = "pages",
                        upsert: bool = True) -> Dict[str, Any]:
    """
    Save a list of page dicts into MongoDB. Uses 'url' as the unique key and performs
    replace_one(upsert=True) if upsert is True. Returns a summary dict with counts and errors.
    If uri is None or empty, MONGO_URI environment variable will be used.

    Example:
      from db import save_pages_to_mongo
      summary = save_pages_to_mongo(pages, uri="mongodb://localhost:27017", db_name="sitecrawler", collection_name="pages")
    """
    summary = {"inserted": 0, "updated": 0, "errors": []}
    client = None
    try:
        client = get_mongo_client(uri)
        # quick server check
        client.admin.command("ping")
        db = client[db_name]
        coll = db[collection_name]

        # ensure unique index on url (idempotent)
        try:
            coll.create_index("url", unique=True)
        except Exception as e:
            logger.debug("Could not create index on url: %s", e)

        for p in pages:
            try:
                doc = dict(p)
                # optional: add metadata like saved timestamp
                # from datetime import datetime
                # doc["_saved_at"] = datetime.utcnow()
                if upsert:
                    res = coll.replace_one({"url": doc.get("url")}, doc, upsert=True)
                    # note: depending on driver, res.matched_count > 0 indicates update; res.upserted_id indicates insert
                    if getattr(res, "matched_count", 0) > 0:
                        summary["updated"] += 1
                    else:
                        # If matched_count == 0 and upserted_id exists, it was an insert
                        if getattr(res, "upserted_id", None) is not None:
                            summary["inserted"] += 1
                        else:
                            # fallback increment
                            summary["inserted"] += 1
                else:
                    coll.insert_one(doc)
                    summary["inserted"] += 1
            except Exception as e:
                logger.exception("Error saving document for url=%s: %s", p.get("url"), e)
                summary["errors"].append({"url": p.get("url"), "error": str(e)})
    except PyMongoError as e:
        logger.exception("MongoDB connection or operation failed: %s", e)
        summary["errors"].insert(0, {"connection_error": str(e)})
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        summary["errors"].insert(0, {"configuration_error": str(e)})
    finally:
        if client:
            client.close()
    return summary

def load_pages_from_mongo(uri: Optional[str] = None,
                         db_name: str = "sitecrawler",
                         collection_name: str = "pages",
                         limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Load documents (pages) from MongoDB collection and return as a list of dicts.
    Use limit to restrict the number of documents returned (helpful for large collections).
    If uri is None or empty, MONGO_URI environment variable will be used.

    Example:
      pages = load_pages_from_mongo(uri="mongodb://localhost:27017", db_name="sitecrawler", collection_name="pages", limit=1000)
    """
    client = None
    docs = []
    try:
        client = get_mongo_client(uri)
        client.admin.command("ping")
        db = client[db_name]
        coll = db[collection_name]
        cursor = coll.find({}, projection=None)
        if limit:
            cursor = cursor.limit(limit)
        for d in cursor:
            # convert ObjectId and other non-serializable fields as needed (leave as-is for in-memory use)
            # Remove Mongo-internal _id if you prefer
            if "_id" in d:
                d.pop("_id")
            docs.append(d)
    except Exception as e:
        logger.exception("Failed loading pages from MongoDB: %s", e)
        raise
    finally:
        if client:
            client.close()
    return docs
