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
# DEBUG: Test Connection
try:
    client = pymongo.MongoClient(st.secrets["mongo"]["uri"], serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    st.sidebar.success("✅ MongoDB Connected Successfully!")
except Exception as e:
    st.sidebar.error(f"❌ Connection Error: {e}")

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro", page_icon="🕸️", layout="wide")

# Custom CSS for Pastel Colors and Minimalist UI
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
        color: #4A90E2; /* Pastel Blue */
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

# --- SECURE AUTHENTICATION SETUP ---

def setup_google_auth():
    """
    Sets up Google Auth internally using secrets. 
    NO UI input required.
    """
    if "google" in st.secrets and "credentials" in st.secrets["google"]:
        try:
            # Create a temp file for the credentials JSON
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(st.secrets["google"]["credentials"])
                temp_cred_path = f.name
            
            # Set env var strictly on the backend
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_path
            return True
        except Exception:
            return False
    return False

# Initialize Auth silently
google_auth_status = setup_google_auth()

# --- MONGODB CONNECTION ---
@st.cache_resource(show_spinner=False)
def init_mongo_connection():
    """
    Connects to MongoDB using SECRETS only. 
    This function never exposes the URI to the frontend.
    """
    try:
        # Load strictly from secrets
        uri = st.secrets["mongo"]["uri"]
        client = pymongo.MongoClient(uri)
        
        # Test connection immediately
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
            st.error("Missing 'db' or 'collection' in secrets.toml")
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
    
    domain = urlparse(start_url).netloc
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    while queue and count < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        
        visited.add(url)
        count += 1
        
        progress_bar.progress(count / max_pages)
        status_text.text(f"Crawling: {url}")
        
        try:
            start_time = time.time()
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            latency = (time.time() - start_time) * 1000
            
            page_data = {
                "url": url,
                "domain": domain,
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
                page_data['title'] = soup.title.string if soup.title else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'] if meta_desc else ""
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
                    page_data['images'].append({
                        'src': img.get('src'),
                        'alt': img.get('alt', '')
                    })

                # Indexability
                robots_meta = soup.find('meta', attrs={'name': 'robots'})
                if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
                    page_data['indexable'] = False

                # Links
                for link in soup.find_all('a', href=True):
                    abs_link = urljoin(url, link['href'])
                    abs_link = abs_link.split('#')[0].rstrip('/')
                    
                    if urlparse(abs_link).netloc == domain:
                        page_data['links'].append(abs_link)
                        if abs_link not in visited and abs_link not in queue:
                            queue.append(abs_link)
                            
            # Upsert into Mongo
            collection.update_one(
                {"url": url}, 
                {"$set": page_data}, 
                upsert=True
            )

        except Exception as e:
            # Log error
            collection.update_one(
                {"url": url},
                {"$set": {"url": url, "status_code": 0, "error": str(e), "crawl_time": datetime.now()}},
                upsert=True
            )

    progress_bar.progress(100)
    status_text.success("Crawl Complete!")

# --- ANALYZER ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None
    
    df = pd.DataFrame(list(col.find({}, {'_id': 0})))
    if df.empty: return None
    
    metrics = {}
    metrics['total_pages'] = len(df)
    
    if 'content_hash' in df.columns:
        metrics['duplicate_content'] = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
    else:
        metrics['duplicate_content'] = pd.DataFrame()

    metrics['duplicate_titles'] = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
    metrics['duplicate_desc'] = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
    
    def check_canonical(row):
        return row['canonical'] and row['canonical'] != row['url']
    metrics['canonical_issues'] = df[df.apply(check_canonical, axis=1)] if 'canonical' in df.columns else pd.DataFrame()
    
    missing_alt_urls = []
    if 'images' in df.columns:
        for idx, row in df.iterrows():
            if isinstance(row['images'], list):
                for img in row['images']:
                    if not img.get('alt'):
                        missing_alt_urls.append(row['url'])
                        break
    metrics['missing_alt'] = df[df['url'].isin(missing_alt_urls)]

    metrics['status_3xx'] = df[(df['status_code'] >= 300) & (df['status_code'] < 400)]
    metrics['status_4xx'] = df[(df['status_code'] >= 400) & (df['status_code'] < 500)]
    metrics['status_5xx'] = df[df['status_code'] >= 500]
    metrics['indexable'] = df[df['indexable'] == True]
    metrics['non_indexable'] = df[df['indexable'] == False]
    metrics['slow_pages'] = df[df['latency_ms'] > 2000]

    return metrics, df

# --- VISUALIZER ---
def generate_network_graph(df):
    G = nx.DiGraph()
    for _, row in df.iterrows():
        G.add_node(row['url'], title=row['title'], group=row['status_code'])
        if isinstance(row['links'], list):
            for link in row['links']:
                if link in df['url'].values:
                    G.add_edge(row['url'], link)
    return G

# --- UI COMPONENTS ---
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
                st.dataframe(df_subset[['url', 'status_code']], use_container_width=True)

# --- SIDEBAR CONTROL PANEL ---
with st.sidebar:
    st.header("🎮 Control Panel")
    
    # --- INTERNAL CONNECTION STATUS ONLY ---
    # No input fields here, just simple indicators
    col_status1, col_status2 = st.columns(2)
    
    with col_status1:
        if init_mongo_connection():
            st.success("DB: Online")
        else:
            st.error("DB: Offline")
            
    with col_status2:
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
    
    if st.button("🗑️ Clear Database"):
        coll = get_db_collection()
        if coll is not None:
            coll.delete_many({})
            st.success("Database Cleared")
            time.sleep(1)
            st.rerun()

# --- MAIN TABS ---
tab1, tab2, tab3 = st.tabs(["📊 Crawl Report", "🕸️ Site Structure", "🔍 Search"])

# Load Data
metrics_data, full_df = get_metrics() if get_db_collection() and get_db_collection().count_documents({}) > 0 else (None, None)

with tab1:
    if metrics_data:
        st.subheader("Site Health Overview")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Total Pages", metrics_data['total_pages'], full_df, "tot")
        with c2: render_metric_card("Indexable", len(metrics_data['indexable']), metrics_data['indexable'], "idx")
        with c3: render_metric_card("Non-Indexable", len(metrics_data['non_indexable']), metrics_data['non_indexable'], "nidx")
        with c4: render_metric_card("Slow (>2s)", len(metrics_data['slow_pages']), metrics_data['slow_pages'], "slow")

        st.subheader("Content Issues")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Dup. Content", len(metrics_data['duplicate_content']), metrics_data['duplicate_content'], "dup_c")
        with c2: render_metric_card("Dup. Titles", len(metrics_data['duplicate_titles']), metrics_data['duplicate_titles'], "dup_t")
        with c3: render_metric_card("Dup. Desc", len(metrics_data['duplicate_desc']), metrics_data['duplicate_desc'], "dup_d")
        with c4: render_metric_card("Canonical Err", len(metrics_data['canonical_issues']), metrics_data['canonical_issues'], "canon")

        st.subheader("Technical Issues")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Missing Alt", len(metrics_data['missing_alt']), metrics_data['missing_alt'], "alt")
        with c2: render_metric_card("3xx Redirects", len(metrics_data['status_3xx']), metrics_data['status_3xx'], "3xx")
        with c3: render_metric_card("4xx Errors", len(metrics_data['status_4xx']), metrics_data['status_4xx'], "4xx")
        with c4: render_metric_card("5xx Errors", len(metrics_data['status_5xx']), metrics_data['status_5xx'], "5xx")
    else:
        st.info("No crawl data found. Enter a URL in the sidebar and click 'Start Crawl'.")

with tab2:
    if full_df is not None:
        st.subheader("Site Architecture Map")
        G = generate_network_graph(full_df)
        net = Network(height='600px', width='100%', bgcolor='#222222', font_color='white')
        net.from_nx(G)
        net.toggle_physics(True)
        
        path = tempfile.gettempdir() + "/network.html"
        net.save_graph(path)
        with open(path, 'r', encoding='utf-8') as f:
            st.components.v1.html(f.read(), height=600)
    else:
        st.warning("Data needed for visualization.")

with tab3:
    st.subheader("Deep Search")
    query = st.text_input("Search content...")
    if query and get_db_collection():
        col = get_db_collection()
        results = list(col.find({"page_text": {"$regex": query, "$options": "i"}}))
        st.write(f"Found {len(results)} matches")
        for res in results:
            with st.expander(f"{res.get('title')} - {res['url']}"):
                st.write(f"**Description:** {res.get('meta_desc')}")
                st.write(f"[Open Link]({res['url']})")
