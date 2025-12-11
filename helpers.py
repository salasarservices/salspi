import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pymongo
import tempfile
import time
import hashlib
import os
import json
import urllib3
from datetime import datetime

# --- SAFE IMPORTS ---
try:
    from google.cloud import language_v1
    NLP_AVAILABLE = True
except ImportError:
    NLP_AVAILABLE = False

try:
    import textrazor
    TEXTRAZOR_AVAILABLE = True
except ImportError:
    TEXTRAZOR_AVAILABLE = False

try:
    import cloudscraper
    SCRAPER_AVAILABLE = True
except ImportError:
    SCRAPER_AVAILABLE = False

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- AUTHENTICATION (FIXED) ---
def setup_google_auth():
    """
    Sets up Google Auth by creating a temporary JSON file from Streamlit secrets.
    CRITICAL FIX: Normalizes newlines in the private_key.
    """
    if "google" in st.secrets and "credentials" in st.secrets["google"]:
        try:
            creds = st.secrets["google"]["credentials"]
            
            # 1. Handle if secrets are returned as a string (rare but possible)
            if isinstance(creds, str):
                try: creds = json.loads(creds)
                except json.JSONDecodeError: return False
            
            # 2. Convert AttrDict to standard Dict
            creds_dict = dict(creds)
            
            # 3. CRITICAL FIX: Fix the Private Key Newlines
            # Streamlit TOML sometimes interprets \n as a literal backslash-n string.
            # We replace literal "\\n" with actual newlines "\n" so json.dump escapes them correctly.
            if "private_key" in creds_dict:
                pk = creds_dict["private_key"]
                creds_dict["private_key"] = pk.replace("\\n", "\n")

            # 4. Dump to file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
                json.dump(creds_dict, f)
                temp_cred_path = f.name
            
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_path
            return True
        except Exception as e:
            # st.error(f"Auth Error: {e}") # Uncomment for debugging
            return False
    return False

def setup_textrazor_auth():
    if TEXTRAZOR_AVAILABLE and "textrazor" in st.secrets and "api_key" in st.secrets["textrazor"]:
        textrazor.api_key = st.secrets["textrazor"]["api_key"]
        return True
    return False

# --- DATABASE ---
@st.cache_resource(show_spinner=False)
def init_mongo_connection():
    try:
        uri = st.secrets["mongo"]["uri"]
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception: return None

def get_db_collection():
    client = init_mongo_connection()
    if client:
        try: return client[st.secrets["mongo"]["db"]][st.secrets["mongo"]["collection"]]
        except KeyError: return None
    return None

# --- CRAWLER LOGIC ---
def get_page_hash(content):
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def normalize_url(url):
    try:
        parsed = urlparse(url)
        clean = parsed._replace(fragment="").geturl()
        return clean.rstrip('/')
    except: return url

def crawl_site(start_url, max_pages):
    collection = get_db_collection()
    if collection is None:
        st.error("Database unavailable.")
        return
    
    collection.delete_many({})
    start_url = normalize_url(start_url)
    base_domain = urlparse(start_url).netloc.replace('www.', '')
    
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    while queue and count < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        count += 1
        progress_bar.progress(count / max_pages)
        status_text.text(f"Crawling {count}/{max_pages}: {url}")
        
        try:
            time.sleep(0.1)
            start_time = time.time()
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            latency = (time.time() - start_time) * 1000
            final_url = normalize_url(response.url)
            
            page_data = {
                "url": final_url, "domain": base_domain, "status_code": response.status_code,
                "content_type": response.headers.get('Content-Type', ''), "crawl_time": datetime.now(),
                "latency_ms": latency, "links": [], "images": [], "title": "", "meta_desc": "",
                "canonical": "", "page_text": "", "content_hash": "", "indexable": True, "h1_count": 0, "word_count": 0
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta['content'].strip() if meta and meta.get('content') else ""
                canon = soup.find('link', rel='canonical')
                page_data['canonical'] = canon['href'] if canon else ""
                page_data['h1_count'] = len(soup.find_all('h1'))
                
                for s in soup(["script", "style"]): s.extract()
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['page_text'] = text_content
                page_data['content_hash'] = get_page_hash(text_content)
                page_data['word_count'] = len(text_content.split())
                
                for img in soup.find_all('img'):
                    if img.get('src'):
                        page_data['images'].append({'src': urljoin(url, img.get('src')), 'alt': img.get('alt', '')})
                
                robots = soup.find('meta', attrs={'name': 'robots'})
                if robots and 'noindex' in robots.get('content', '').lower(): page_data['indexable'] = False
                
                for link in soup.find_all('a', href=True):
                    raw = link['href'].strip()
                    if not raw or raw.startswith(('mailto:', 'tel:', 'javascript:', '#')): continue
                    abs_link = normalize_url(urljoin(url, raw))
                    if base_domain in urlparse(abs_link).netloc:
                        page_data['links'].append(abs_link)
                        if abs_link not in visited and abs_link not in queue: queue.append(abs_link)

            collection.update_one({"url": final_url}, {"$set": page_data}, upsert=True)
        except Exception as e:
            collection.update_one({"url": url}, {"$set": {"url": url, "status_code": 0, "error": str(e)}}, upsert=True)

    progress_bar.progress(100)
    status_text.success(f"Crawl Complete! Visited {count} pages.")

# --- METRICS CALCULATOR ---
def get_metrics_df():
    col = get_db_collection()
    if col is None: return None
    data = list(col.find({}, {'page_text': 0, '_id': 0}))
    df = pd.DataFrame(data)
    if df.empty: return None
    
    cols = ['url', 'title', 'meta_desc', 'canonical', 'images', 'status_code', 'content_hash', 'latency_ms', 'indexable', 'h1_count', 'word_count']
    for c in cols: 
        if c not in df.columns: df[c] = None
        
    df['title'] = df['title'].fillna("")
    df['meta_desc'] = df['meta_desc'].fillna("")
    df['content_hash'] = df['content_hash'].fillna("")
    df['canonical'] = df['canonical'].fillna("")
    df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce').fillna(0)
    df['h1_count'] = pd.to_numeric(df['h1_count'], errors='coerce').fillna(0)
    df['word_count'] = pd.to_numeric(df['word_count'], errors='coerce').fillna(0)
    
    return df

# --- NLP & SCRAPER ---
def analyze_google(text):
    if not NLP_AVAILABLE: return None, "Library missing."
    try:
        client = language_v1.LanguageServiceClient()
        if not text or len(text.split()) < 20: return None, "Text too short (<20 words)."
        doc = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(request={'document': doc}).document_sentiment
        entities = client.analyze_entities(request={'document': doc}).entities
        return {"sentiment": sentiment, "entities": entities}, None
    except Exception as e: return None, str(e)

def analyze_textrazor(text, auth_status):
    if not TEXTRAZOR_AVAILABLE: return None, "TextRazor Library missing."
    if not auth_status: return None, "TextRazor API Key missing."
    try:
        client = textrazor.TextRazor(extractors=["entities", "topics"])
        if not text or len(text.strip()) < 50: return None, "Text too short for TextRazor."
        response = client.analyze(text)
        return response, None
    except Exception as e: return None, str(e)

# --- ROBUST SCRAPER (BYPASSES STATUS 247/403) ---
def scrape_external_page(url):
    # 1. Try CloudScraper (Primary method for Anti-Bot)
    if SCRAPER_AVAILABLE:
        try:
            # Create a scraper that mimics a desktop Chrome browser
            scraper = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'mobile': False
                }
            )
            resp = scraper.get(url, timeout=20)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                for s in soup(["script", "style", "nav", "footer", "iframe", "noscript"]): s.extract()
                return soup.get_text(separator=' ', strip=True), None
        except Exception:
            pass # Fallback to standard requests if cloudscraper errors out

    # 2. Fallback: Requests with Full Browser Headers
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        session = requests.Session()
        resp = session.get(url, headers=headers, timeout=15, verify=False)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            for s in soup(["script", "style", "nav", "footer", "iframe", "noscript"]): s.extract()
            return soup.get_text(separator=' ', strip=True), None
            
        return None, f"Failed to fetch: Status {resp.status_code}"
    except Exception as e: return None, str(e)
