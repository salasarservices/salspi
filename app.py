import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pymongo
import networkx as nx
from pyvis.network import Network
import tempfile
import time
import hashlib
import os
from datetime import datetime

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro", page_icon="🕸️", layout="wide")

st.markdown("""
<style>
    .metric-card {
        background-color: #F0F2F6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        margin-bottom: 10px;
        border: 1px solid #E0E0E0;
    }
    .metric-value {
        font-size: 32px;
        font-weight: bold;
        color: #4A90E2; 
    }
    .metric-label {
        font-size: 14px;
        color: #666;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# --- AUTHENTICATION ---
def setup_google_auth():
    if "google" in st.secrets and "credentials" in st.secrets["google"]:
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(st.secrets["google"]["credentials"])
                temp_cred_path = f.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_path
            return True
        except Exception:
            return False
    return False

google_auth_status = setup_google_auth()

# --- MONGODB CONNECTION ---
@st.cache_resource(show_spinner=False)
def init_mongo_connection():
    try:
        uri = st.secrets["mongo"]["uri"]
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        return None

def get_db_collection():
    client = init_mongo_connection()
    if client:
        try:
            db_name = st.secrets["mongo"]["db"]
            coll_name = st.secrets["mongo"]["collection"]
            db = client[db_name]
            return db[coll_name]
        except KeyError:
            return None
    return None

# --- CRAWLER ENGINE ---
def get_page_hash(content):
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def crawl_site(start_url, max_pages):
    collection = get_db_collection()
    if collection is None:
        st.error("Database unavailable. Please check server logs.")
        return
    
    # Normalize Base Domain (Ignore www)
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc.replace('www.', '')
    
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    while queue and count < max_pages:
        url = queue.pop(0)
        url = url.split('#')[0].rstrip('/')
        
        if url in visited: continue
        visited.add(url)
        count += 1
        
        progress_bar.progress(count / max_pages)
        status_text.text(f"Crawling ({count}/{max_pages}): {url}")
        
        try:
            time.sleep(0.05) # Tiny delay to prevent blocking
            start_time = time.time()
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}, timeout=10)
            latency = (time.time() - start_time) * 1000
            
            page_data = {
                "url": url,
                "domain": base_domain,
                "status_code": response.status_code,
                "content_type": response.headers.get('Content-Type', ''),
                "crawl_time": datetime.now(),
                "latency_ms": latency,
                "links": [],
                "images": [],
                "title": "",
                "meta_desc": "",
                "canonical": "",
                "word_count": 0,
                "content_hash": "",
                "indexable": True,
                "page_text": ""
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Metadata
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'].strip() if meta_desc and meta_desc.get('content') else ""
                canonical = soup.find('link', rel='canonical')
                page_data['canonical'] = canonical['href'] if canonical else ""
                
                # Content & Hash
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['word_count'] = len(text_content.split())
                page_data['content_hash'] = get_page_hash(text_content)
                page_data['page_text'] = text_content
                
                # Images
                imgs = soup.find_all('img')
                for img in imgs:
                    src = img.get('src')
                    if src:
                        page_data['images'].append({
                            'src': urljoin(url, src),
                            'alt': img.get('alt', '')
                        })

                # Indexability
                robots_meta = soup.find('meta', attrs={'name': 'robots'})
                if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
                    page_data['indexable'] = False

                # Links
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    raw_link = link['href'].strip()
                    
                    # Check if link is valid
                    if not raw_link or raw_link.startswith(('mailto:', 'tel:', 'javascript:', '#')):
                        continue
                    
                    abs_link = urljoin(url, raw_link)
                    clean_link = abs_link.split('#')[0].rstrip('/')
                    
                    link_parsed = urlparse(clean_link)
                    link_domain = link_parsed.netloc.replace('www.', '')
                    
                    if link_domain == base_domain:
                        page_data['links'].append(clean_link)
                        if clean_link not in visited and clean_link not in queue:
                            queue.append(clean_link)
                            
            # Save to Mongo
            collection.update_one({"url": url}, {"$set": page_data}, upsert=True)

        except Exception as e:
            collection.update_one(
                {"url": url},
                {"$set": {"url": url, "status_code": 0, "error": str(e), "crawl_time": datetime.now()}},
                upsert=True
            )

    progress_bar.progress(100)
    status_text.success(f"Crawl Complete! Visited {count} pages.")

# --- ANALYZER (OPTIMIZED) ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None, None
    
    # Exclude 'page_text' to prevent memory crash
    data = list(col.find({}, {'_id': 0, 'page_text': 0}))
    df = pd.DataFrame(data)
    
    if df.empty: return None, None

    # Robust Data Cleaning
    expected_cols = [
        'url', 'title', 'meta_desc', 'canonical', 'images', 
        'status_code', 'content_hash', 'indexable', 'latency_ms'
    ]
    for c in expected_cols:
        if c not in df.columns: df[c] = None

    df['meta_desc'] = df['meta_desc'].fillna("")
    df['title'] = df['title'].fillna("")
    df['canonical'] = df['canonical'].fillna("")
    df['content_hash'] = df['content_hash'].fillna("")
    
    metrics = {}
    metrics['total_pages'] = len(df)
    
    if 'content_hash' in df.columns:
        metrics['duplicate_content'] = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
    else:
        metrics['duplicate_content'] = pd.DataFrame()

    metrics['duplicate_titles'] = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
    metrics['duplicate_desc'] = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
    
    def check_canonical(row):
        if not row['canonical']: return False
        return row['canonical'] != row['url']
    metrics['canonical_issues'] = df[df.apply(check_canonical, axis=1)]
    
    missing_alt_urls = []
    if 'images' in df.columns:
        for idx, row in df.iterrows():
            if isinstance(row['images'], list):
                for img in row['images']:
                    if isinstance(img, dict) and not img.get('alt'):
                        missing_alt_urls.append(row['url'])
                        break
    metrics['missing_alt'] = df[df['url'].isin(missing_alt_urls)]

    df['status_code'] = pd.to_numeric(df['status_code'], errors='coerce').fillna(0)
    metrics['status_3xx'] = df[(df['status_code'] >= 300) & (df['status_code'] < 400)]
    metrics['status_4xx'] = df[(df['status_code'] >= 400) & (df['status_code'] < 500)]
    metrics['status_5xx'] = df[df['status_code'] >= 500]
    
    metrics['indexable'] = df[df['indexable'] == True]
    metrics['non_indexable'] = df[df['indexable'] == False]
    
    df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce').fillna(0)
    metrics['slow_pages'] = df[df['latency_ms'] > 2000]

    return metrics, df

# --- VISUALIZER ---
def generate_network_graph(df):
    G = nx.DiGraph()
    # Limit nodes to avoid browser crash
    limit_df = df.head(100) 
    
    for _, row in limit_df.iterrows():
        G.add_node(row['url'], title=row['title'], group=row['status_code'])
        if isinstance(row['links'], list):
            for link in row['links']:
                if link in limit_df['url'].values:
                    G.add_edge(row['url'], link)
    return G

# --- UI HELPER ---
def render_metric_card(label, value, df_subset, key_suffix):
    with st.container():
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)
        if value > 0:
            with st.expander(f"View Details"):
                # Pagination - only show top 50 rows
                st.dataframe(df_subset[['url', 'status_code']].head(50), use_container_width=True)
                if len(df_subset) > 50:
                    st.caption(f"Showing top 50 of {len(df_subset)} records to prevent lag.")

# --- SIDEBAR ---
with st.sidebar:
    st.header("🎮 Control Panel")
    
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if init_mongo_connection():
            st.success("DB: Online")
        else:
            st.error("DB: Offline")
    with col_s2:
        if google_auth_status:
            st.success("Auth: Active")
        else:
            st.warning("Auth: Inactive")

    st.markdown("---")
    target_url = st.text_input("Target URL", "https://example.com")
    max_pages_limit = st.number_input("Max Pages", 10, 2000, 50)
    
    if st.button("🚀 Start Crawl", type="primary"):
        crawl_site(target_url, max_pages_limit)
        st.rerun()
    
    if st.button("🗑
