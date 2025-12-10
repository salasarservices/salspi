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
            time.sleep(0.05)
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

            # Fixed Syntax Error Here: Joined onto one line
            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'].strip() if meta_desc and meta_desc.get('content') else ""
                canonical = soup.find('link', rel='canonical')
                page_data['canonical'] = canonical['href'] if canonical else ""
                
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['word_count'] = len(text_content.split())
                page_data['content_hash'] = get_page_hash(text_content)
                page_data['page_text'] = text_content
                
                imgs = soup.find_all('img')
                for img in imgs:
                    src = img.get('src')
                    if src:
                        page_data['images'].append({
                            'src': urljoin(url, src),
                            'alt': img.get('alt', '')
                        })

                robots_meta = soup.find('meta', attrs={'name': 'robots'})
                if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
                    page_data['indexable'] = False

                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    raw_link = link['href'].strip()
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
                            
            collection.update_one({"url": url}, {"$set": page_data}, upsert=True)

        except Exception as e:
            collection.update_one(
                {"url": url},
                {"$set": {"url": url, "status_code": 0, "error": str(e), "crawl_time": datetime.now()}},
                upsert=True
            )

    progress_bar.progress(100)
    status_text.success(f"Crawl Complete! Visited {count} pages.")

# --- ANALYZER ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None, None
    
    data = list(col.find({}, {'_id': 0, 'page_text': 0}))
    df = pd.DataFrame(data)
    
    if df.empty: return None, None

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
                st.dataframe(df_subset[['url', 'status_code']].head(50), use_container_width=True)
                if len(df_subset) > 50:
                    st.caption(f"Showing top 50 of {len(df_subset)} records.")

# --- SIDEBAR ---
with st.sidebar:
    st.header("Control Panel")
    
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
    
    if st.button("Start Crawl", type="primary"):
        crawl_site(target_url, max_pages_limit)
        st.rerun()
    
    if st.button("Clear Database"):
        coll = get_db_collection()
        if coll is not None:
            coll.delete_many({})
            st.success("Database Cleared")
            time.sleep(1)
            st.rerun()

# --- MAIN LOGIC ---
tab1, tab2, tab3 = st.tabs(["Crawl Report", "Site Structure", "Deep Search"])

col = get_db_collection()
has_data = False
if col is not None:
    try:
        if col.count_documents({}, limit=1) > 0:
            has_data = True
    except Exception:
        has_data = False

if has_data:
    metrics_data, full_df = get_metrics()
else:
    metrics_data, full_df = None, None

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
        st.info("Displaying connections for top 100 pages.")
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

# --- DEEP SEARCH TAB ---
with tab3:
    st.subheader("Search Website Content")
    st.markdown("Search for specific words or phrases inside the **text content** of crawled pages.")
    
    query = st.text_input("Enter word or phrase:", placeholder="e.g. refund policy")
    
    search_col = get_db_collection()
    if query and search_col is not None:
        try:
            # 1. Search strictly in 'page_text' with regex
            # Limit to 50 to prevent browser crash, but fetch enough to be useful
            results = list(search_col.find(
                {"page_text": {"$regex": query, "$options": "i"}},
                {"url": 1, "page_text": 1, "_id": 0}
            ).limit(50))
            
            if results:
                st.success(f"✅ Found **{len(results)}** pages containing '{query}'")
                
                # Prepare data for clean table
                search_data = []
                for res in results:
                    # Create a snippet of text around the match
                    text = res.get('page_text', '')
                    idx = text.lower().find(query.lower())
                    
                    if idx != -1:
                        start = max(0, idx - 40)
                        end = min(len(text), idx + len(query) + 40)
                        snippet = "..." + text[start:end] + "..."
                    else:
                        snippet = "(Match found in hidden text)"
                        
                    search_data.append({
                        "Found On URL": res['url'],
                        "Context Snippet": snippet
                    })
                
                st.dataframe(pd.DataFrame(search_data), use_container_width=True)
            else:
                st.warning(f"No pages found containing '{query}'.")
                
        except Exception as e:
            st.error(f"Search failed: {e}")
