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
        # allow either a single uri entry or separate user/password + host fields
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
    Also URL-encode credentials if provided separately (not needed if full uri already).
    """
    if not uri:
        raise ValueError("No Mongo URI provided. Put it in Streamlit secrets or set MONGO_URI env var.")
    if "<" in uri or ">" in uri or "password" in uri.lower() and "@" in uri:
        # This detects strings like mongodb+srv://user:<password>@...
        raise ValueError(
            "Your Mongo URI contains a placeholder like <password>. Replace it with the real password "
            "in .streamlit/secrets.toml or an environment variable. If the password contains special characters, "
            "URL-encode it (use urllib.parse.quote_plus)."
        )
    return uri

def get_config():
    """Return resolved config (uri, db, collection)."""
    cfg = _config_from_secrets_or_env()
    return cfg

def get_client(timeout_ms=5000):
    """
    Create and return a MongoClient connected to the configured URI.
    Will raise if connection cannot be established.
    """
    cfg = _config_from_secrets_or_env()
    uri = cfg.get("uri")
    uri = _validate_uri(uri)
    # Pass the URI directly; for mongodb+srv, tls is on by default
    client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
    # Force server selection to raise early if unreachable
    client.server_info()
    return client

def is_connected():
    try:
        c = get_client(timeout_ms=2000)
        c.close()
        return True
    except Exception:
        return False

def save_crawl(site, crawl):
    """
    Save crawl. Try MongoDB first; fallback to local JSON file if unreachable.
    Returns an id (str): inserted_id (stringified) or path to local file.
    """
    doc = {
        "site": site,
        "timestamp": datetime.utcnow().isoformat() + "Z",
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
        existing.append(doc)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        return path

def latest_crawl(site):
    """
    Return the latest crawl document for site. Try MongoDB first, then fallback to local JSON.
    """
    cfg = _config_from_secrets_or_env()
    try:
        client = get_client()
        db = client[cfg["db"]]
        col = db[cfg["collection"]]
        doc = col.find_one({"site": site}, sort=[("timestamp", -1)])
        client.close()
        return doc
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
    cfg = _config_from_secrets_or_env()
    try:
        client = get_client()
        db = client[cfg["db"]]
        col = db[cfg["collection"]]
        docs = list(col.find({"site": site}).sort("timestamp", -1).limit(limit))
        client.close()
        return docs
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
