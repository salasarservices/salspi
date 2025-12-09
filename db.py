import os
import logging
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger("site_crawler_db")
logger.setLevel(logging.INFO)

def _resolve_uri(uri: Optional[str]) -> Optional[str]:
    if uri:
        return uri
    env_uri = os.getenv("MONGO_URI")
    if env_uri:
        return env_uri
    return None

def get_mongo_client(uri: Optional[str], server_selection_timeout_ms: int = 5000) -> MongoClient:
    resolved = _resolve_uri(uri)
    if not resolved:
        raise ValueError("No MongoDB URI provided. Set uri argument or MONGO_URI env var, or configure Streamlit secrets.")
    return MongoClient(resolved, serverSelectionTimeoutMS=server_selection_timeout_ms)

def save_pages_to_mongo(pages: List[Dict[str, Any]],
                        uri: Optional[str] = None,
                        db_name: str = "sitecrawler",
                        collection_name: str = "pages",
                        upsert: bool = True) -> Dict[str, Any]:
    summary = {"inserted": 0, "updated": 0, "errors": []}
    client = None
    try:
        client = get_mongo_client(uri)
        client.admin.command("ping")
        db = client[db_name]
        coll = db[collection_name]
        try:
            coll.create_index("url", unique=True)
        except Exception as e:
            logger.debug("Could not create index on url: %s", e)

        for p in pages:
            try:
                doc = dict(p)
                if upsert:
                    res = coll.replace_one({"url": doc.get("url")}, doc, upsert=True)
                    if getattr(res, "matched_count", 0) > 0:
                        summary["updated"] += 1
                    else:
                        if getattr(res, "upserted_id", None) is not None:
                            summary["inserted"] += 1
                        else:
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
