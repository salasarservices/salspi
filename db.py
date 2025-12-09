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
    # try Streamlit secrets (only if running inside Streamlit and st is available)
    try:
        import streamlit as st  # type: ignore
        if hasattr(st, "secrets"):
            mongo = st.secrets.get("mongo", {}) or {}
            if mongo.get("uri"):
                return mongo.get("uri")
    except Exception:
        pass
    return None


def get_mongo_client(uri: Optional[str], server_selection_timeout_ms: int = 5000) -> MongoClient:
    """
    Return a MongoClient for the provided URI. Raises ValueError if uri is None.
    """
    resolved = _resolve_uri(uri)
    if not resolved:
        raise ValueError("No MongoDB URI provided. Set uri argument, MONGO_URI env var, or st.secrets['mongo']['uri'].")
    return MongoClient(resolved, serverSelectionTimeoutMS=server_selection_timeout_ms)


def save_pages_to_mongo(
    pages: List[Dict[str, Any]],
    uri: Optional[str] = None,
    db_name: str = "sitecrawler",
    collection_name: str = "pages",
    upsert: bool = True,
) -> Dict[str, Any]:
    """
    Save page documents into collection_name. Uses 'url' as the unique key.
    Returns a summary dict containing counts and any errors.
    """
    summary = {"inserted": 0, "updated": 0, "errors": []}
    client = None
    try:
        client = get_mongo_client(uri)
        client.admin.command("ping")
        db = client[db_name]
        coll = db[collection_name]

        # ensure index on url
        try:
            coll.create_index("url", unique=True)
        except Exception as e:
            logger.debug("Could not create index on url: %s", e)

        for p in pages:
            try:
                doc = dict(p)
                if upsert:
                    res = coll.replace_one({"url": doc.get("url")}, doc, upsert=True)
                    # res.matched_count > 0 means a document existed (update), otherwise insert
                    if getattr(res, "matched_count", 0) > 0:
                        summary["updated"] += 1
                    else:
                        # some drivers set upserted_id when an insert happens
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


def save_ocr_to_mongo(
    pages: List[Dict[str, Any]],
    uri: Optional[str] = None,
    db_name: str = "sitecrawler",
    collection_name: str = "ocr-data",
    upsert: bool = True,
) -> Dict[str, Any]:
    """
    Save OCR results (per-page aggregated and per-image details) into a separate collection.
    Documents are upserted by url and have structure:
      {
        "url": "...",
        "ocr_text": "...",
        "images": [ {"src": "...", "alt": "...", "ocr_text": "...", "ocr_error": "..."}, ... ]
      }
    """
    summary = {"inserted": 0, "updated": 0, "errors": []}
    client = None
    try:
        client = get_mongo_client(uri)
        client.admin.command("ping")
        db = client[db_name]
        coll = db[collection_name]

        # ensure unique url index
        try:
            coll.create_index("url", unique=True)
        except Exception as e:
            logger.debug("Could not create index on url in OCR collection: %s", e)

        for p in pages:
            try:
                url = p.get("url")
                doc = {
                    "url": url,
                    "ocr_text": p.get("ocr_text", "") or "",
                    "images": [],
                }
                for img in p.get("ocr_details", []):
                    doc["images"].append(
                        {
                            "src": img.get("src"),
                            "alt": img.get("alt"),
                            "ocr_text": img.get("ocr_text", "") or "",
                            "ocr_error": img.get("ocr_error"),
                        }
                    )
                if upsert:
                    res = coll.replace_one({"url": url}, doc, upsert=True)
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
                logger.exception("Error saving OCR document for url=%s: %s", p.get("url"), e)
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


def load_pages_from_mongo(
    uri: Optional[str] = None, db_name: str = "sitecrawler", collection_name: str = "pages", limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Load documents from MongoDB collection and return as list of dicts.
    Removes _id fields for easier in-memory handling.
    """
    client = None
    docs: List[Dict[str, Any]] = []
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
