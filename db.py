import os
import json
from datetime import datetime
from urllib.parse import quote_plus

# Try to import streamlit.secrets when running inside Streamlit.
try:
    import streamlit as _st  # only used to access _st.secrets if available
    _SECRETS = getattr(_st, "secrets", None)
except Exception:
    _st = None
    _SECRETS = None

from pymongo import MongoClient, errors
from bson import ObjectId
from bson.errors import InvalidId

# Local fallback directory used when MongoDB is unreachable
LOCAL_FALLBACK_DIR = os.environ.get("LOCAL_CRAWL_DIR", "local_crawls")

def _config_from_secrets_or_env():
    """
    Return a dict with keys: uri, db, collection
    Priority:
     1) streamlit secrets [mongo] if available
     2) Environment variables: MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION
    """
    cfg = {"uri": None, "db": None, "collection": None}
    # 1) streamlit secrets (if running inside Streamlit)
    if _SECRETS and "mongo" in _SECRETS:
        sm = _SECRETS["mongo"]
        cfg["uri"] = sm.get("uri") or None
        cfg["db"] = sm.get("db") or sm.get("database") or os.environ.get("MONGO_DB_NAME")
        cfg["collection"] = sm.get("collection") or os.environ.get("MONGO_COLLECTION")
    # 2) environment variables
    if not cfg["uri"]:
        cfg["uri"] = os.environ.get("MONGO_URI")
    if not cfg["db"]:
        cfg["db"] = os.environ.get("MONGO_DB_NAME", "seo_crawler_db")
    if not cfg["collection"]:
        cfg["collection"] = os.environ.get("MONGO_COLLECTION", "crawls")
    return cfg

def _ensure_local_dir():
    if not os.path.exists(LOCAL_FALLBACK_DIR):
        os.makedirs(LOCAL_FALLBACK_DIR, exist_ok=True)

def _local_file_for_site(site):
    safe = site.replace("://", "_").replace("/", "_")
    return os.path.join(LOCAL_FALLBACK_DIR, f"crawl_{safe}.json")

def _validate_uri(uri):
    """
    Basic validation: check common placeholder patterns and provide helpful error.
    """
    if not uri:
        raise ValueError("No Mongo URI provided. Put it in Streamlit secrets or set MONGO_URI env var.")
    if "<" in uri or ">" in uri or ("<password" in uri.lower()):
        raise ValueError(
            "Your Mongo URI contains a placeholder like <password>. Replace it with the real password "
            "in .streamlit/secrets.toml or an environment variable. URL-encode special characters in the password."
        )
    return uri

def get_config():
    """Return resolved config (uri, db, collection)."""
    return _config_from_secrets_or_env()

def get_client(timeout_ms=5000):
    """
    Create and return a MongoClient connected to the configured URI.
    Will raise if connection cannot be established.
    """
    cfg = _config_from_secrets_or_env()
    uri = cfg.get("uri")
    uri = _validate_uri(uri)
    client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
    client.server_info()  # force early exception if unreachable
    return client

def is_connected():
    try:
        c = get_client(timeout_ms=2000)
        c.close()
        return True
    except Exception:
        return False

def _make_json_serializable(obj):
    """
    Recursively convert BSON types (ObjectId, datetime) to JSON serializable Python types.
    Leaves other types as-is.
    """
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat() + "Z"
    if isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_serializable(v) for v in obj]
    # other BSON types (Decimal128, Binary, etc.) can be added if needed
    return obj

def _serialize_doc(doc):
    """
    Convert a single MongoDB doc or local doc to JSON-serializable form.
    """
    if doc is None:
        return None
    # If the doc is a pymongo Cursor/SON, convert to dict-like then process
    try:
        return _make_json_serializable(doc)
    except Exception:
        # As a fallback, attempt a naive json dump/load to coerce types
        try:
            return json.loads(json.dumps(doc, default=str))
        except Exception:
            return doc

def save_crawl(site, crawl):
    """
    Save crawl. Try MongoDB first; fallback to local JSON file if unreachable.
    Returns an id (str): inserted_id (stringified) or path to local file.
    """
    doc = {
        "site": site,
        "timestamp": datetime.utcnow(),
        "crawl": crawl
    }
    cfg = _config_from_secrets_or_env()
    try:
        client = get_client()
        db = client[cfg["db"]]
        col = db[cfg["collection"]]
        res = col.insert_one(doc)
        client.close()
        return str(res.inserted_id)
    except Exception:
        # Fallback to file
        _ensure_local_dir()
        path = _local_file_for_site(site)
        existing = []
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                existing = []
        # store a serializable version (convert datetime to ISO)
        serial_doc = {
            "site": site,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "crawl": crawl
        }
        existing.append(serial_doc)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        return path

def latest_crawl(site):
    """
    Return the latest crawl document for site. Try MongoDB first, then fallback to local JSON.
    Always returns JSON-serializable dict (ObjectId and datetime converted).
    """
    cfg = _config_from_secrets_or_env()
    try:
        client = get_client()
        db = client[cfg["db"]]
        col = db[cfg["collection"]]
        doc = col.find_one({"site": site}, sort=[("timestamp", -1)])
        client.close()
        return _serialize_doc(doc)
    except Exception:
        path = _local_file_for_site(site)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f)
            if not arr:
                return None
            arr_sorted = sorted(arr, key=lambda d: d.get("timestamp", ""), reverse=True)
            return arr_sorted[0]
        except Exception:
            return None

def list_crawls(site, limit=10):
    """
    Return a list of recent crawl documents for `site`. Always JSON-serializable.
    """
    cfg = _config_from_secrets_or_env()
    try:
        client = get_client()
        db = client[cfg["db"]]
        col = db[cfg["collection"]]
        docs = list(col.find({"site": site}).sort("timestamp", -1).limit(limit))
        client.close()
        return [_serialize_doc(d) for d in docs]
    except Exception:
        path = _local_file_for_site(site)
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f)
            arr_sorted = sorted(arr, key=lambda d: d.get("timestamp", ""), reverse=True)
            return arr_sorted[:limit]
        except Exception:
            return []

def delete_database(confirm=False):
    """
    Dangerous: drop the configured DB. If MongoDB is unavailable, delete local fallback files.
    Must pass confirm=True to actually perform the operation.
    """
    if not confirm:
        raise ValueError("Must pass confirm=True to actually delete the database.")
    cfg = _config_from_secrets_or_env()
    try:
        client = get_client()
        client.drop_database(cfg["db"])
        client.close()
        return True
    except Exception:
        # fallback: remove local fallback files for safety
        _ensure_local_dir()
        try:
            for fname in os.listdir(LOCAL_FALLBACK_DIR):
                if fname.startswith("crawl_"):
                    os.remove(os.path.join(LOCAL_FALLBACK_DIR, fname))
            return True
        except Exception:
            return False
